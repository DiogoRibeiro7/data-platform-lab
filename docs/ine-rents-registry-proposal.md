# INE new-lease rents: canonical source proposal

Status: **blocked on canonical registry promotion and a trusted handoff**.
This proposal selects a real source and defines the intended consumer contract;
it does not enable INE ingestion, curation, PostgreSQL publication or serving.

The registry audit on 2026-11-14 used commit
`8023c7f3b3ef5ebc1c4d46983fc407a944a231b3` of `DiogoRibeiro7/data`.
Its [INE record][registry-record] is an `external-reference`, with
`storage: authoritative-upstream`. Its [promotion guidance][registry-readme]
requires a concrete reusable snapshot and a pinned consumer contract. There is
no canonical INE dataset or handoff at that commit. The audit commit must **not**
be supplied as a canonical INE source pin.

## Proposed source

Proposed registry ID: `ine-portugal-new-lease-rents` (not yet registered).
Publisher: Instituto Nacional de Estatística, I.P. (Statistics Portugal).

| Property | Selected value |
| --- | --- |
| Indicator | `0014698`, median rent for new family-dwelling leases, methodology 2026 |
| Observation period | Annual, **2025 only**, API period code `S7A2025` |
| Measure | Nominal monthly contracted rent per m² of gross private area; not asking rent or total household rent |
| Unit | EUR/m²/month; source precision two decimal places, scale exponent zero |
| Geography | Portugal, NUTS I–III, municipalities and selected parishes; NUTS 2024, classification `05433` |
| Upstream update | `2026-09-29` in the candidate response |
| Collection | Inventory date `2026-10-30`; response `DataExtracao` is `2026-10-30T15:16:42.184Z` |

The exact [observation request][observation-url] selects one period and all
available geographies. [Official metadata][metadata-url], inspected on
2026-11-14, exposes 2020–2025 and records a September 2026 correction affecting
some NUTS III values. This proposal excludes the other years. Retrieval time
and update date do not make the mutable API an immutable release; replay must
use approved stored bytes. Keep methodology 2026 separate from older series.

### Existing snapshot candidate

A real response is already committed in `social-security-housing-investment`.
Read the **Git blob**, not that repository's potentially modified working file.
The [committed inventory][candidate-inventory] records the request, hash, size,
date, units and collection context.

| Identity | Value |
| --- | --- |
| Candidate repository commit | `f84577d7cb5a5dd0338707faa32fe166881d9596` |
| Candidate source path | `data/snapshots/rents/ine_0014698_S7A2025_2026-10-30.json` |
| Git blob ID | `500ab19d99028aa3484227a35ff3714b763afaa7` |
| SHA-256 of exact JSON bytes | `e19aa7476bc3c21f0525b753b774c8a71f87e6e02bfb049adffcb43607377285` |
| Size | 91,363 bytes, uncompressed UTF-8 JSON |
| Audit result | 638 distinct geography records for 2025: 487 numeric values and 151 missing values carrying `sinal_conv: "-"` |

The [candidate blob][candidate-blob] matches the inventory's SHA-256 and byte
count. Those counts describe **all source levels**, not 638 municipalities.
They are a local source audit, not platform output or acceptance evidence.
No observation API request was made for this proposal. Neither the response
nor generated data is copied into this repository.

Reproduce the audit offline in an existing trusted checkout of that research
repository (Python standard library only; reads Git objects without changing
the checkout):

```python
import hashlib
import json
import subprocess

commit = "f84577d7cb5a5dd0338707faa32fe166881d9596"
path = "data/snapshots/rents/ine_0014698_S7A2025_2026-10-30.json"
raw = subprocess.check_output(["git", "show", f"{commit}:{path}"])
assert len(raw) == 91363
assert hashlib.sha256(raw).hexdigest() == (
    "e19aa7476bc3c21f0525b753b774c8a71f87e6e02bfb049adffcb43607377285"
)
(response,) = json.loads(raw)
assert response["IndicadorCod"] == "0014698"
assert set(response["Dados"]) == {"2025"}
rows = response["Dados"]["2025"]
assert len(rows) == len({row["geocod"] for row in rows}) == 638
assert sum("valor" in row for row in rows) == 487
assert sum(row.get("sinal_conv") == "-" for row in rows) == 151
```

### Rights and interpretation

The [registry rights record][registry-record] permits redistribution of directly
retrieved INE statistical information under [CC BY 4.0][licence]. INE's
[published usage notice][ine-rights] also specifies CC BY 4.0 with clear source
attribution. Preserve the indicator, reference period, collection/update dates,
licence link and transformation description with each output. Suggested credit:
“Fonte: Instituto Nacional de Estatística, I.P. – Portugal”; add the indicator
and any modifications. This approval does not extend to PORDATA exports.

The [research source notes][candidate-notes] flag a sales-versus-rents wording
inconsistency in INE's formula documentation. Registry review must retain that
note and check the methodology; the platform must not silently rewrite source
metadata. A median per m² cannot be summed or averaged into a regional rent,
nor converted to affordability without a compatible income and dwelling basis.

## Reuse the existing extractor

The existing R package is [`ineDataClient` 0.1.9][client-description], audited at
commit `e60e69ea3a4f149b61b3b2b44c49e8c01be6095a`. Its MIT licence covers code;
the data licence above is independent.

For a separately approved acquisition, reuse `ine_client()` and
`ine_data_raw()` with indicator `0014698` and fixed `Dim1 = "S7A2025"`.
The [data implementation][client-data] validates indicator/filter identity and
retains exact response bytes in `ine_raw_body`. For larger acquisitions,
`ine_plan()`, `ine_download()`, `ine_manifest()` and `ine_load()` already provide
bounded requests, checkpoints and checksum validation. Its
[provenance contract][client-provenance] explicitly leaves revision status
unverified; a successful download is not proof of an atomic upstream release.

The existing candidate's extraction is recorded by its research inventory;
this proposal does not attribute those historical bytes to `ineDataClient`.
Do not introduce a Python INE HTTP wrapper or require R in the platform runtime.
The platform should consume approved JSON through the existing
[registry handoff and BlobStore/S3 path](registry-handoff-ingestion.md).

## Registry promotion requested

Prepare a separately reviewed change in `DiogoRibeiro7/data`, following its
[data policy][registry-policy]. This platform PR makes no upstream changes.

1. Review the exact candidate and confirm the proposed ID, rights, attribution
   and methodology. Add canonical `README.md` and schema-valid `metadata.yaml`
   under `datasets/ine-portugal-new-lease-rents/`, retaining the original source
   request and the candidate repository/commit/path as acquisition provenance.
2. Promote the exact candidate bytes under `raw/`, preserving the SHA-256 above.
   If a different response is selected, record a new hash and collection date
   and update this proposal; never label fresh API bytes as the old snapshot.
3. Archive and hash the indicator metadata, geography codebook/levels, missing
   value definitions and relevant methodology notes used to interpret the
   response. These companion artifacts are **not yet pinned**. Metadata read
   during this audit is current metadata, not a historical metadata snapshot.
   Review its compatibility with the October response explicitly.
4. Register this platform consumer against an exact reviewed implementation
   commit when available. Keep runtime receipts and Silver/Gold outputs outside
   the source registry; register any other consumers separately.
5. After approval/merge, record the new full registry commit and generate the
   existing [v1 handoff][handoff-contract] with both `--verify-local` and
   `--verify-checkout`. Supply the trusted repository, dataset and commit
   independently to the consumer. Retain hashes for every declared artifact and
   the emitted handoff; verify the complete file set before execution.

There is currently no canonical INE commit, metadata/codebook hash, handoff hash
or platform model version to fill in. An external record, research Git commit
or this proposal cannot substitute for those identities.

## Proposed platform contract after approval

These are implementation requirements, **not available commands or tables**.
Use the established ingestion receipts, immutable publication evidence and
release lifecycle. The current curator, publication schemas and serving API are
ONS-specific: admitting an INE raw object does not make them INE-compatible.

| Layer | Proposed contract |
| --- | --- |
| Bronze | Exact canonical JSON and companion artifacts in the existing content-addressed BlobStore; verify all hashes before promotion and retain source/licence lineage. |
| Silver | One typed row per `(indicator_code, period_code, geography_version, geography_code)` within a pinned vintage. Keep full codes as strings, source labels, metadata-defined geography level, year, methodology, unit, raw `valor`/`ind_string`, nullable `DECIMAL(18,2)` value and original flag/description. Reject duplicates, malformed decimals and unsupported dimensions. |
| Gold | Municipal annual median new-lease rent per m²: select metadata level 5 without aggregating medians. Retain every municipality present in the approved source, including nulls; validate coverage against the pinned codebook. Do not manufacture absent municipalities or infer levels from code length. |
| Vintage | Dataset ID, exact registry commit, source and companion hashes, model version, source update and collection dates; later revisions create a separate vintage. |
| Publication | Proposed relation `dpl_curated.ine_new_lease_rents`; share plan/reconciliation/evidence logic through a narrow generalization and a new numbered migration, preserving existing ONS contracts and reader permissions. |

The candidate's `-` flag means null/not applicable; it must not become zero or
be relabelled confidential. Future suppressed or provisional flags need their
own explicit supported mapping. Preserve raw strings and flag descriptions;
reject unknown flags instead of guessing. Keep nominal values and geographic
editions separate.

Future parameterized read example, to implement and test once the proposed
relation and accepted-vintage guard exist (DB-API named parameters):

```sql
SELECT geography_code, geography_name, reference_year,
       median_rent_eur_m2_month, source_flag
FROM dpl_curated.ine_new_lease_rents
WHERE dataset_id = %(dataset_id)s
  AND registry_commit = %(registry_commit)s
  AND source_sha256 = %(source_sha256)s
  AND model_version = %(model_version)s
  AND reference_year = %(reference_year)s
ORDER BY geography_code;
```

The future reader must first verify the selected accepted publication and its
full artifact identity, following the [PostgreSQL reader contract](ons-pay-postgres-research.md).
The query alone is not an acceptance check. `social-security-housing-investment`,
`portugal-housing-generations` and `empirical-analysis-r` could reuse the
municipal observations through that read-only contract, recording query pins
with results. No consumer migration is part of this proposal.

Before enabling the adapter, add bounded synthetic fixture cases for duplicate
cells, malformed dimensions, geographic levels, decimal/flag preservation,
schema drift and revision isolation. Verify exact-source replay, idempotent
publication, full-vintage reconciliation, failed/partial-write rejection and
side-effect-free preview using disposable services. Genuine validation remains
opt-in, requires the approved handoff and explicit apply, and must report actual
counts/fingerprints. No INE publication or acceptance result exists yet.

[registry-record]: https://github.com/DiogoRibeiro7/data/blob/8023c7f3b3ef5ebc1c4d46983fc407a944a231b3/external/ine-portugal-housing-statistics/metadata.yaml
[registry-readme]: https://github.com/DiogoRibeiro7/data/blob/8023c7f3b3ef5ebc1c4d46983fc407a944a231b3/external/ine-portugal-housing-statistics/README.md
[registry-policy]: https://github.com/DiogoRibeiro7/data/blob/8023c7f3b3ef5ebc1c4d46983fc407a944a231b3/docs/DATA_POLICY.md
[handoff-contract]: https://github.com/DiogoRibeiro7/data/blob/8023c7f3b3ef5ebc1c4d46983fc407a944a231b3/docs/INGESTION_HANDOFF.md
[observation-url]: https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0014698&Dim1=S7A2025&lang=PT
[metadata-url]: https://www.ine.pt/ine/json_indicador/pindicaMeta.jsp?varcd=0014698&lang=PT
[candidate-blob]: https://gitlab.com/DiogoRibeiro7/social-security-housing-investment/-/blob/f84577d7cb5a5dd0338707faa32fe166881d9596/data/snapshots/rents/ine_0014698_S7A2025_2026-10-30.json
[candidate-inventory]: https://gitlab.com/DiogoRibeiro7/social-security-housing-investment/-/blob/f84577d7cb5a5dd0338707faa32fe166881d9596/data/sources/rent_affordability_sources.json
[candidate-notes]: https://gitlab.com/DiogoRibeiro7/social-security-housing-investment/-/blob/f84577d7cb5a5dd0338707faa32fe166881d9596/docs/data-sources.md
[licence]: https://creativecommons.org/licenses/by/4.0/
[ine-rights]: https://www.ine.pt/ine_novidades/semin/INEWS66/4/
[client-description]: https://gitlab.com/DiogoRibeiro7/ine-data-client/-/blob/e60e69ea3a4f149b61b3b2b44c49e8c01be6095a/DESCRIPTION
[client-data]: https://gitlab.com/DiogoRibeiro7/ine-data-client/-/blob/e60e69ea3a4f149b61b3b2b44c49e8c01be6095a/R/data.R
[client-provenance]: https://gitlab.com/DiogoRibeiro7/ine-data-client/-/blob/e60e69ea3a4f149b61b3b2b44c49e8c01be6095a/docs/provenance.md
