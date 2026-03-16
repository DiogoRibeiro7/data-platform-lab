import { describe, test, mock, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  fetchPage,
  fetchAllPages,
  transformPosts,
  runApiPipeline,
} from "../src/ingestion/api-pipeline.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Helper: create a temporary directory and return its path.
 */
function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "api-pipeline-test-"));
}

/**
 * Helper: build a mock Response-like object.
 */
function mockResponse(body, { ok = true, status = 200, statusText = "OK" } = {}) {
  return {
    ok,
    status,
    statusText,
    json: async () => body,
  };
}

// ---------------------------------------------------------------------------
// Global fetch mock setup
// ---------------------------------------------------------------------------

const mockFetch = mock.fn();
const originalFetch = globalThis.fetch;

beforeEach(() => {
  globalThis.fetch = mockFetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  mockFetch.mock.resetCalls();
  mockFetch.mock.restore();
});

// ---------------------------------------------------------------------------
// fetchPage
// ---------------------------------------------------------------------------

describe("fetchPage", () => {
  test("successful fetch parses JSON response", async () => {
    const posts = [
      { userId: 1, id: 1, title: "first", body: "body one" },
      { userId: 1, id: 2, title: "second", body: "body two" },
    ];

    mockFetch.mock.mockImplementation(async () => mockResponse(posts));

    const result = await fetchPage("https://example.com/posts", {
      offset: 0,
      limit: 10,
    });

    assert.equal(result.length, 2);
    assert.equal(result[0].title, "first");
    assert.equal(mockFetch.mock.callCount(), 1);

    // Verify the URL includes pagination params
    const calledUrl = mockFetch.mock.calls[0].arguments[0];
    assert.ok(calledUrl.includes("_start=0"));
    assert.ok(calledUrl.includes("_limit=10"));
  });

  test("HTTP error throws", async () => {
    mockFetch.mock.mockImplementation(async () =>
      mockResponse(null, { ok: false, status: 404, statusText: "Not Found" }),
    );

    await assert.rejects(
      () => fetchPage("https://example.com/posts", { offset: 0, limit: 10 }),
      (error) => {
        assert.ok(error.message.includes("404"));
        return true;
      },
    );
  });

  test("timeout behavior", async () => {
    mockFetch.mock.mockImplementation(async () => {
      const error = new Error("The operation was aborted due to timeout");
      error.name = "TimeoutError";
      throw error;
    });

    await assert.rejects(
      () => fetchPage("https://example.com/posts", { offset: 0, limit: 10, timeoutMs: 100 }),
      (error) => {
        assert.equal(error.name, "TimeoutError");
        return true;
      },
    );

    // Should have retried 3 times total (initial + 2 retries)
    assert.equal(mockFetch.mock.callCount(), 3);
  });

  test("malformed JSON throws", async () => {
    mockFetch.mock.mockImplementation(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => {
        throw new SyntaxError("Unexpected token");
      },
    }));

    await assert.rejects(
      () => fetchPage("https://example.com/posts", { offset: 0, limit: 10 }),
      (error) => {
        assert.ok(error instanceof SyntaxError);
        return true;
      },
    );
  });

  test("retries on 5xx server errors", async () => {
    let callCount = 0;
    mockFetch.mock.mockImplementation(async () => {
      callCount++;
      if (callCount <= 2) {
        return mockResponse(null, {
          ok: false,
          status: 503,
          statusText: "Service Unavailable",
        });
      }
      return mockResponse([{ userId: 1, id: 1, title: "ok", body: "ok" }]);
    });

    const result = await fetchPage("https://example.com/posts");
    assert.equal(result.length, 1);
    assert.equal(mockFetch.mock.callCount(), 3);
  });
});

// ---------------------------------------------------------------------------
// fetchAllPages
// ---------------------------------------------------------------------------

describe("fetchAllPages", () => {
  test("accumulates records across pages", async () => {
    mockFetch.mock.mockImplementation(async (url) => {
      const parsed = new URL(url);
      const start = Number(parsed.searchParams.get("_start"));
      const limit = Number(parsed.searchParams.get("_limit"));

      // Return `limit` records for every page (simulating full pages)
      const records = Array.from({ length: limit }, (_, i) => ({
        userId: 1,
        id: start + i + 1,
        title: `Post ${start + i + 1}`,
        body: "body",
      }));

      return mockResponse(records);
    });

    const { records, pagesFetched } = await fetchAllPages(
      "https://example.com/posts",
      { pageSize: 3, maxPages: 4 },
    );

    assert.equal(pagesFetched, 4);
    assert.equal(records.length, 12); // 3 records x 4 pages
  });

  test("stops when page returns fewer records than limit", async () => {
    let callCount = 0;
    mockFetch.mock.mockImplementation(async () => {
      callCount++;
      if (callCount <= 2) {
        // Full pages
        return mockResponse(
          Array.from({ length: 5 }, (_, i) => ({
            userId: 1,
            id: (callCount - 1) * 5 + i + 1,
            title: `Post ${(callCount - 1) * 5 + i + 1}`,
            body: "body",
          })),
        );
      }
      // Partial page — triggers stop
      return mockResponse([
        { userId: 1, id: 100, title: "Last", body: "body" },
      ]);
    });

    const { records, pagesFetched } = await fetchAllPages(
      "https://example.com/posts",
      { pageSize: 5, maxPages: 10 },
    );

    assert.equal(pagesFetched, 3);
    assert.equal(records.length, 11); // 5 + 5 + 1
  });
});

// ---------------------------------------------------------------------------
// transformPosts
// ---------------------------------------------------------------------------

describe("transformPosts", () => {
  test("transforms valid posts to canonical schema", () => {
    const raw = [
      { userId: 1, id: 1, title: "Hello World", body: "This is the body text" },
    ];

    const result = transformPosts(raw);

    assert.equal(result.length, 1);
    assert.equal(result[0].id, 1);
    assert.equal(result[0].userId, 1);
    assert.equal(result[0].title, "Hello World");
    assert.equal(result[0].titleLength, 11);
    assert.equal(result[0].bodyPreview, "This is the body text");
    assert.equal(result[0].wordCount, 5);
  });

  test("skips records missing required fields", () => {
    const raw = [
      { userId: 1, id: 1, title: "Valid", body: "Has all fields" },
      { userId: 2, id: 2, title: "No body" }, // missing body
      { id: 3, title: "No userId", body: "test" }, // missing userId
      { userId: 4, title: "No id", body: "test" }, // missing id
      { userId: 5, id: 5, body: "No title" }, // missing title
    ];

    const result = transformPosts(raw);

    assert.equal(result.length, 1);
    assert.equal(result[0].id, 1);
  });

  test("skips records with null required fields", () => {
    const raw = [
      { userId: 1, id: null, title: "t", body: "b" },
      { userId: null, id: 2, title: "t", body: "b" },
      { userId: 3, id: 3, title: "t", body: "b" },
    ];

    const result = transformPosts(raw);

    assert.equal(result.length, 1);
    assert.equal(result[0].id, 3);
  });

  test("truncates body preview to 100 chars", () => {
    const longBody = "a".repeat(200);
    const raw = [
      { userId: 1, id: 1, title: "Test", body: longBody },
    ];

    const result = transformPosts(raw);

    assert.equal(result[0].bodyPreview.length, 100);
    assert.equal(result[0].bodyPreview, "a".repeat(100));
  });
});

// ---------------------------------------------------------------------------
// runApiPipeline
// ---------------------------------------------------------------------------

describe("runApiPipeline", () => {
  test("full pipeline creates files and returns correct summary", async () => {
    const tempDir = makeTempDir();
    const rawDir = join(tempDir, "raw");
    const processedDir = join(tempDir, "processed");

    const posts = [
      { userId: 1, id: 1, title: "First Post", body: "Body of the first post" },
      { userId: 1, id: 2, title: "Second Post", body: "Body of the second post" },
      { userId: 2, id: 3, title: "Third Post", body: "Body of the third post" },
    ];

    // Return 3 records (less than pageSize of 10) so pagination stops after 1 page
    mockFetch.mock.mockImplementation(async () => mockResponse(posts));

    try {
      const summary = await runApiPipeline({
        baseUrl: "https://example.com/posts",
        rawDir,
        processedDir,
        pageSize: 10,
        maxPages: 5,
      });

      assert.equal(summary.pagesFetched, 1);
      assert.equal(summary.totalRecords, 3);
      assert.equal(summary.recordsWritten, 3);
      assert.deepStrictEqual(summary.errors, []);
      assert.ok(summary.runId.length > 0);
      assert.ok(summary.durationSeconds >= 0);
      assert.equal(summary.apiUrl, "https://example.com/posts");

      // Verify raw file was written
      const rawContent = JSON.parse(readFileSync(summary.rawPath, "utf-8"));
      assert.equal(rawContent.length, 3);
      assert.equal(rawContent[0].title, "First Post");

      // Verify processed file was written
      const processedContent = JSON.parse(
        readFileSync(summary.processedPath, "utf-8"),
      );
      assert.equal(processedContent.length, 3);
      assert.ok("titleLength" in processedContent[0]);
      assert.ok("bodyPreview" in processedContent[0]);
      assert.ok("wordCount" in processedContent[0]);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  test("handles API failure gracefully", async () => {
    const tempDir = makeTempDir();
    const rawDir = join(tempDir, "raw");
    const processedDir = join(tempDir, "processed");

    mockFetch.mock.mockImplementation(async () =>
      mockResponse(null, { ok: false, status: 500, statusText: "Internal Server Error" }),
    );

    try {
      const summary = await runApiPipeline({
        baseUrl: "https://example.com/posts",
        rawDir,
        processedDir,
        pageSize: 10,
        maxPages: 1,
      });

      assert.ok(summary.errors.length > 0);
      assert.equal(summary.totalRecords, 0);
      assert.equal(summary.recordsWritten, 0);
      assert.equal(summary.pagesFetched, 0);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
