from __future__ import annotations

import os
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback
    load_dotenv = None


@dataclass(slots=True)
class AppSettings:
    project_root: Path
    data_lake_root: Path
    weather_city_name: str
    weather_latitude: float
    weather_longitude: float
    weather_timezone: str

    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str

    weather_sync_interval_minutes: int
    quality_checks_config: Path

    telegram_bot_token: str
    telegram_allowed_chat_ids: set[int]

    def resolve_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path
        return (self.project_root / path).resolve()



def _read_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    return float(raw)



def _read_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    return int(raw)



def _read_set_of_ints(key: str) -> set[int]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return set()
    values = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.add(int(item))
    return values



def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _maybe_load_dotenv(project_root: Path) -> None:
    if load_dotenv is None:
        return
    load_dotenv(project_root / ".env", override=False)


def _validate_positive_int(value: int, name: str) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")
    return value


def _validate_settings(settings: AppSettings) -> AppSettings:
    settings.weather_sync_interval_minutes = _validate_positive_int(
        settings.weather_sync_interval_minutes,
        "WEATHER_SYNC_INTERVAL_MINUTES",
    )
    return settings


def _build_settings() -> AppSettings:
    project_root = _project_root()
    _maybe_load_dotenv(project_root)

    settings = AppSettings(
        project_root=project_root,
        data_lake_root=Path(os.getenv("DATA_LAKE_ROOT", "data/lakehouse")),
        weather_city_name=os.getenv("WEATHER_CITY_NAME", "New York"),
        weather_latitude=_read_float("WEATHER_LATITUDE", 40.7128),
        weather_longitude=_read_float("WEATHER_LONGITUDE", -74.0060),
        weather_timezone=os.getenv("WEATHER_TIMEZONE", "America/New_York"),
        influxdb_url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        influxdb_token=os.getenv("INFLUXDB_TOKEN", "change-me"),
        influxdb_org=os.getenv("INFLUXDB_ORG", "fabric-project"),
        influxdb_bucket=os.getenv("INFLUXDB_BUCKET", "weather_timeseries"),
        weather_sync_interval_minutes=_read_int("WEATHER_SYNC_INTERVAL_MINUTES", 30),
        quality_checks_config=Path(os.getenv("QUALITY_CHECKS_CONFIG", "config/quality_checks.yaml")),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_allowed_chat_ids=_read_set_of_ints("TELEGRAM_ALLOWED_CHAT_IDS"),
    )
    settings.data_lake_root = settings.resolve_path(settings.data_lake_root)
    settings.quality_checks_config = settings.resolve_path(settings.quality_checks_config)
    return _validate_settings(settings)


@lru_cache(maxsize=1)
def _cached_settings() -> AppSettings:
    return _build_settings()


def load_settings(*, reload: bool = False) -> AppSettings:
    if reload:
        _cached_settings.cache_clear()
    return _cached_settings()
