# ADMAP Deployment Notes

## Architecture

```
Browser
  -> ADMAP ingress (admap.example.com)
     -> /dashboard/*   -> dashboard-service (Angular)
     -> /merfisheyes/* -> merfisheyes (this repo, Next.js, basePath=/merfisheyes)
     -> /api/dataset/* -> file-service
file-service -> LakeFS/MinIO -> returns presigned URL -> browser fetches it directly
```

The viewer reads ADMAP datasets via `owner`, `dataset`, `version`, and
optional `datasetRoot` query params. Each file fetch goes through
`/api/dataset/presign-download?filePath=...` with a bearer token from
`localStorage.access_token`, falling back to
`/api/dataset/public-presign-download` for public datasets.

Canonical entry URLs (ADMAP Dataset Detail page should link to these):

```
/merfisheyes/viewer?owner=X&dataset=Y&version=Z       # single cell
/merfisheyes/sm-viewer?owner=X&dataset=Y&version=Z    # single molecule
```

`/viewer/from-s3?...` and `/sm-viewer/from-s3?...` still work but only as
client-side redirect shims — don't link to them from new code.

## CORS (important)

The presigned URL points at the **object store**, not the ADMAP origin, so
the final `fetch()` is cross-origin:

```
origin:  https://admap.example.com
target:  http://<minio-host>:9000/<bucket>/<key>?X-Amz-...
```

CORS must be configured **on the object store**, not in this repo.

### MinIO (dev and k8s)

Set `MINIO_API_CORS_ALLOW_ORIGIN` to the ADMAP ingress origin (not the
MinIO host port):

```yaml
# docker-compose
services:
  minio:
    environment:
      - MINIO_API_CORS_ALLOW_ORIGIN=http://localhost:8080,http://localhost:18080
```

```yaml
# k8s
env:
  - name: MINIO_API_CORS_ALLOW_ORIGIN
    value: "https://admap.example.com"
```

ADMAP ships two compose files with different host port mappings; check
which one you're running:

| compose                | MinIO API port | Console |
| ---------------------- | -------------- | ------- |
| `texera-lakefs.yml`    | 9000           | 9001    |
| `file-service` standalone | 9500        | 9501    |

### AWS S3 backend

If LakeFS's blockstore is real S3, set CORS on the bucket:

```json
[{
  "AllowedHeaders": ["*"],
  "AllowedMethods": ["GET", "HEAD"],
  "AllowedOrigins": ["https://admap.example.com"],
  "MaxAgeSeconds": 3600
}]
```

## Dataset format

Datasets must be pre-chunked by the Python scripts under `scripts/` before
upload — the viewer does no preprocessing.

## Image build

```bash
docker buildx build --platform linux/amd64 \
  -t <registry>/merfisheyes:<tag> --push .
```

Local builds on Apple Silicon default to `linux/arm64` and will fail to run
on amd64 nodes — `--platform linux/amd64` is required.
