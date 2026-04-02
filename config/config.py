# =========================
# config/config.py
# =========================
from dataclasses import dataclass
import os

@dataclass
class Config:
    catalog: str
    schema: str
    raw_path: str
    checkpoint: str 
    landing: str 


def get_config(env: str, layer: str) -> Config:
    url = os.getenv("S3_BUCKET_URL") or "s3://amazn-s3-db-bckt"
    if env == "dev":
        return Config(
            catalog="dev_catalog",
            schema=layer,
            raw_path=f"s3://amazn-s3-db-bckt/{env}/medallion/{layer}/{layer}",
            checkpoint = f"{url}/{env}/checkpoints",
            landing = f"{url}/{env}/landing"
        )
    elif env == "prod":
        return Config(
            catalog="prod",
            schema=layer,
            raw_path=f"s3://amazn-s3-db-bckt/{env}/medallion/{layer}/{layer}",
            checkpoint = f"{url}/{env}/checkpoints",
            landing = f"{url}/{env}/landing"
        )
    else:
        raise ValueError(f"Unknown env: {env}")