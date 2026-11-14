# Cloudflare R2: private raw object storage

The Python S3 adapter can use Cloudflare R2 without a new implementation.
R2 holds raw source bytes; Neon stores published relational data. Curated
Parquet, receipts, publication evidence and local concurrency guards remain on
the local filesystem. No Worker or public bucket URL is required.

## 1. Create the bucket

After activating R2 in the Cloudflare dashboard, run in PowerShell:

```powershell
npx wrangler whoami
npx wrangler r2 bucket list
```

Wrangler may require `npx wrangler login` even when the separate `cf` CLI is
authenticated. If `data-platform-lab-raw` does **not** already exist, create it:

```powershell
npx wrangler r2 bucket create data-platform-lab-raw
npx wrangler r2 bucket list
```

Keep this bucket private. Do not enable an `r2.dev` URL or a public domain.
Use default jurisdiction unless a specific data-residency requirement justifies
an EU- or US-jurisdictional bucket with its corresponding endpoint.

## 2. Create a bucket-scoped S3 token

In [Cloudflare's dashboard](https://dash.cloudflare.com/), open **R2 > Manage
R2 API tokens > Create API token**. Select **Object Read & Write** and restrict
the token to `data-platform-lab-raw`. Save its Access Key ID and Secret Access
Key privately; the secret is shown only once. Obtain the Cloudflare **account
ID**, which is distinct from the access key. Never commit, paste or log secrets.

## 3. Configure the PowerShell process

In the same PowerShell session that will run Poetry:

```powershell
$r2AccountId = (Read-Host "Cloudflare account ID").Trim()
$env:DPL_S3_ENDPOINT_URL = "https://${r2AccountId}.r2.cloudflarestorage.com"
$env:DPL_S3_BUCKET = "data-platform-lab-raw"
$env:AWS_DEFAULT_REGION = "auto"
$env:AWS_ACCESS_KEY_ID = Read-Host "R2 Access Key ID"
$r2Secret = Read-Host "R2 Secret Access Key" -AsSecureString
$r2SecretPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($r2Secret)
try {
    $env:AWS_SECRET_ACCESS_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($r2SecretPointer)
} finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($r2SecretPointer)
}
```

The secure secret prompt avoids terminal echo and command history. These
variables last only for the current process and its children. `poetry run`
inherits them. Never put credentials in a saved JSON job or command-line flag.
R2 accepts `auto` as its S3 signing region. A deliberately EU-jurisdictional
bucket uses `https://<ACCOUNT_ID>.eu.r2.cloudflarestorage.com` instead.

## 4. Verify upload, download, existence and listing

From the repository's `python/` directory:

```powershell
poetry install
poetry run data-platform-lab storage --backend s3 --bucket $env:DPL_S3_BUCKET --endpoint-url $env:DPL_S3_ENDPOINT_URL --region auto --key-prefix _checks --smoke-key r2-roundtrip.txt
```

Expect `Round trip : ok` and `Listing : ok`. This deliberately leaves one
private probe object, `_checks/r2-roundtrip.txt`, in the bucket. It never
touches the `ons/raw` prefix.

Common failures:

- `AccessDenied`: check bucket-specific token scope and the process variables.
- `NoSuchBucket`: confirm the name with `npx wrangler r2 bucket list`.
- Python import failure: run `poetry install` in `python/`; global `pip` does
  not install packages into the Poetry virtual environment.

## 5. Use R2 with the existing Neon workflow

Replace local `--object-root` with these explicit S3 options in
`ons-pay-refresh`, `ons-pay-load-verify`, `ons-pay-transform`, or
`ons-pay-postgres`:

```text
--backend s3
--bucket data-platform-lab-raw
--endpoint-url https://<ACCOUNT_ID>.r2.cloudflarestorage.com
--region auto
--key-prefix ons/raw
```

Keep the independently approved ONS handoff and source pins unchanged. For
`ons-pay-postgres --apply` against Neon's development branch, follow the
[Neon runbook](neon-ons-pay-runbook.md): Parquet and immutable publication
evidence must live on a native POSIX/WSL2 filesystem. PowerShell variables
do not automatically pass into WSL2; supply the S3 credentials privately to
the Linux process as well. Preview before any explicit writes.

## Official references

- [R2 setup](https://developers.cloudflare.com/r2/get-started/)
- [Wrangler and S3 CLI](https://developers.cloudflare.com/r2/get-started/cli/)
- [R2 bucket-scoped credentials](https://developers.cloudflare.com/r2/api/tokens/)
- [Cloudflare boto3 example](https://developers.cloudflare.com/r2/examples/aws/boto3/)
- [Existing S3 raw-storage contract](ons-pay-s3-storage.md)
