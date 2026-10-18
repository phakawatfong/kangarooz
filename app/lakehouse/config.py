"""Where the lake lives: DuckLake catalog (Postgres), data files (S3 / Garage), inlining. From .env."""

from __future__ import annotations

import os
import re

from pydantic import BaseModel, ConfigDict


class LakeConfig(BaseModel):
    """No endpoint or credential defaults: they only come from the environment."""

    model_config = ConfigDict(frozen=True)

    catalog: str                # after "ducklake:", e.g. "postgres:host=... dbname=ducklake" or a local file path
    data_path: str              # "s3://f1-lake/" or a local directory
    inlining_row_limit: int     # batches up to this many rows stay in the catalog until a flush
    s3_endpoint: str | None     # None = AWS S3
    s3_region: str | None
    s3_key_id: str | None       # None = no S3 secret (local data_path)
    s3_secret: str | None
    s3_use_ssl: bool
    s3_url_style: str           # "path" for Garage, "vhost" for AWS

    @classmethod
    def from_env(cls) -> LakeConfig:
        return cls(
            catalog=os.environ["F1_LAKE_CATALOG"],
            data_path=os.environ["F1_LAKE_DATA_PATH"],
            inlining_row_limit=os.environ.get("F1_LAKE_INLINING_ROW_LIMIT", 5000),  # DuckLake's own default is 10
            s3_endpoint=os.environ.get("F1_S3_ENDPOINT"),
            s3_region=os.environ.get("F1_S3_REGION"),
            s3_key_id=os.environ.get("F1_S3_KEY_ID"),
            s3_secret=os.environ.get("F1_S3_SECRET"),
            s3_use_ssl=os.environ.get("F1_S3_USE_SSL", True),
            s3_url_style=os.environ.get("F1_S3_URL_STYLE", "vhost"),
        )

    def describe(self) -> str:
        """For logs: everything except the S3 secret and the catalog password."""
        catalog = re.sub(r"password=\S+", "password=***", self.catalog)
        return (f"catalog={catalog} data_path={self.data_path} inlining_row_limit={self.inlining_row_limit} "
                f"s3_endpoint={self.s3_endpoint} s3_key_id={self.s3_key_id}")
