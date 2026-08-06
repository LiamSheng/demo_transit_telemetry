from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def write_manifest(directory: Path, payload: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)

    attempt_id = payload["attempt_id"]
    path = directory / f"{attempt_id}.json"

    path.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument("--source-url", required=True)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--manifest-path", required=True)

    args = parser.parse_args()

    fetched_at = datetime.now(timezone.utc)
    attempt_id = fetched_at.strftime("%Y%m%dT%H%M%S.%fZ")

    landing_dir = Path(args.landing_path)
    manifest_dir = Path(args.manifest_path)

    request = Request(
        args.source_url,
        headers={
            "User-Agent": "demo-transit-telemetry/0.1",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            http_status = response.status
            body = response.read()
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")

    except HTTPError as exc:
        write_manifest(
            manifest_dir,
            {
                "attempt_id": attempt_id,
                "fetched_at": fetched_at.isoformat(),
                "source_url": args.source_url,
                "http_status": exc.code,
                "publish_status": "FAILED",
                "error_type": "HTTP_ERROR",
                "error_message": str(exc),
            },
        )
        raise

    except URLError as exc:
        write_manifest(
            manifest_dir,
            {
                "attempt_id": attempt_id,
                "fetched_at": fetched_at.isoformat(),
                "source_url": args.source_url,
                "publish_status": "FAILED",
                "error_type": "URL_ERROR",
                "error_message": str(exc.reason),
            },
        )
        raise

    if http_status != 200:
        raise RuntimeError(f"Unexpected HTTP status: {http_status}")

    if not body:
        raise RuntimeError("BC Transit returned an empty response")

    content_sha256 = hashlib.sha256(body).hexdigest()

    file_name = f"alerts_{content_sha256}.pb"
    published_path = landing_dir / file_name

    landing_dir.mkdir(parents=True, exist_ok=True)

    if published_path.exists():
        publish_status = "UNCHANGED"
    else:
        staging_path = landing_dir / f".{file_name}.part"

        staging_path.write_bytes(body)
        staging_path.replace(published_path)

        publish_status = "PUBLISHED"

    manifest = {
        "attempt_id": attempt_id,
        "fetched_at": fetched_at.isoformat(),
        "source_url": args.source_url,
        "http_status": http_status,
        "byte_size": len(body),
        "content_sha256": content_sha256,
        "published_path": str(published_path),
        "publish_status": publish_status,
        "etag": etag,
        "last_modified": last_modified,
    }

    write_manifest(manifest_dir, manifest)

    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
