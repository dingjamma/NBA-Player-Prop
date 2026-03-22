"""
Local file store — same API as the old S3 version.
All data lives under DATA_DIR (default: <project_root>/data/).
"""

import io
import shutil
from pathlib import Path

_ROOT = Path(__file__).parent.parent
DATA_DIR = _ROOT / "data"


def _resolve(key: str) -> Path:
    p = DATA_DIR / key
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def upload_parquet(local_path: Path, key: str) -> str:
    dest = _resolve(key)
    shutil.copy2(str(local_path), str(dest))
    return str(dest)


def upload_text(content: str, key: str, encoding: str = "utf-8") -> str:
    dest = _resolve(key)
    dest.write_text(content, encoding=encoding)
    return str(dest)


def download_parquet(key: str, local_path: Path) -> Path:
    src = DATA_DIR / key
    local_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(local_path))
    return local_path


def read_text(key: str) -> str:
    return (DATA_DIR / key).read_text(encoding="utf-8")


def list_keys(prefix: str) -> list[str]:
    base = DATA_DIR / prefix
    if not base.exists():
        return []
    return [
        p.relative_to(DATA_DIR).as_posix()
        for p in base.rglob("*")
        if p.is_file()
    ]


# ── Compat shim for code that calls _client() / BUCKET directly ───────────────

BUCKET = str(DATA_DIR)


class _LocalClient:
    def download_file(self, bucket, key, dest):
        src = DATA_DIR / key
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), dest)

    def upload_file(self, src, bucket, key):
        dest = _resolve(key)
        shutil.copy2(src, str(dest))

    def get_object(self, Bucket, Key):
        data = (DATA_DIR / Key).read_bytes()
        return {"Body": io.BytesIO(data)}

    def put_object(self, Bucket, Key, Body, **kwargs):
        dest = _resolve(Key)
        if isinstance(Body, (bytes, bytearray)):
            dest.write_bytes(Body)
        else:
            dest.write_bytes(Body.read())


_client_instance = None


def _client():
    global _client_instance
    if _client_instance is None:
        _client_instance = _LocalClient()
    return _client_instance
