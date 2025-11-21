from pathlib import Path

from .userdirs import user_data_dir  # adjust import if needed

DATA_DIR: Path = user_data_dir("aws-reporting")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BRANDS_FILE: Path = DATA_DIR / "brands.json"

UPLOAD_TMP_DIR: Path = DATA_DIR / "tmp_uploads"
UPLOAD_TMP_DIR.mkdir(parents=True, exist_ok=True)


def BRAND_PATH(brand_id: str) -> Path:
    path = DATA_DIR / "brands" / brand_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def BRAND_ARCHIVE_PATH(brand_id: str) -> Path:
    path =  DATA_DIR / "brands" / brand_id / "archive"
    path.mkdir(parents=True, exist_ok=True)
    return path
