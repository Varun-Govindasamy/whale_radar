"""HDFS client wrapper using WebHDFS REST API.

Provides simple read/write/query operations for storing and retrieving
whale events, wallet profiles, signals, and training data.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from hdfs import InsecureClient

from whaleradar.config import settings


class HDFSClient:
    """WebHDFS client for WhaleRadar data persistence."""

    BASE_DIR = "/whaleradar"

    def __init__(self, url: str | None = None) -> None:
        self._client = InsecureClient(url or settings.hdfs_url, user="root")
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        dirs = [
            f"{self.BASE_DIR}/whale_events",
            f"{self.BASE_DIR}/wallet_profiles",
            f"{self.BASE_DIR}/signals",
            f"{self.BASE_DIR}/agent_decisions",
            f"{self.BASE_DIR}/training_data",
            f"{self.BASE_DIR}/models",
            f"{self.BASE_DIR}/reports",
        ]
        for d in dirs:
            self._client.makedirs(d)

    def write_event(self, event: dict) -> str:
        """Write a whale event to HDFS. Returns the file path."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        tx_short = event.get("tx_hash", "unknown")[:12]
        path = f"{self.BASE_DIR}/whale_events/{ts}_{tx_short}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(event, f)
        return path

    def write_signal(self, signal: dict) -> str:
        """Write a generated signal to HDFS."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        sid = signal.get("signal_id", "unknown")
        path = f"{self.BASE_DIR}/signals/{ts}_{sid}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(signal, f)
        return path

    def write_agent_decision(self, decision: dict) -> str:
        """Write an agent decision record to HDFS."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        path = f"{self.BASE_DIR}/agent_decisions/{ts}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(decision, f)
        return path

    def get_wallet_profile(self, address: str) -> dict | None:
        """Retrieve a wallet profile by address. Returns None if not found."""
        path = f"{self.BASE_DIR}/wallet_profiles/{address}.json"
        try:
            with self._client.read(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def save_wallet_profile(self, profile: dict) -> str:
        """Save or update a wallet profile."""
        address = profile["address"]
        path = f"{self.BASE_DIR}/wallet_profiles/{address}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(profile, f)
        return path

    def query_wallet_history(self, address: str) -> list[dict]:
        """Find all whale events involving a specific address."""
        events = []
        try:
            files = self._client.list(f"{self.BASE_DIR}/whale_events")
        except Exception:
            return events

        for fname in files:
            try:
                fpath = f"{self.BASE_DIR}/whale_events/{fname}"
                with self._client.read(fpath, encoding="utf-8") as f:
                    event = json.load(f)
                if event.get("from_address") == address or event.get("to_address") == address:
                    events.append(event)
            except Exception:
                continue
        return events

    def list_signals(self, limit: int = 50) -> list[dict]:
        """List recent signals from HDFS."""
        signals = []
        try:
            files = sorted(self._client.list(f"{self.BASE_DIR}/signals"), reverse=True)[:limit]
        except Exception:
            return signals

        for fname in files:
            try:
                fpath = f"{self.BASE_DIR}/signals/{fname}"
                with self._client.read(fpath, encoding="utf-8") as f:
                    signals.append(json.load(f))
            except Exception:
                continue
        return signals

    def write_training_data(self, records: list[dict], batch_id: str) -> str:
        """Write a batch of training data for model retraining."""
        path = f"{self.BASE_DIR}/training_data/{batch_id}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(records, f)
        return path

    def read_training_data(self) -> list[dict]:
        """Read all training data files and merge into a single list."""
        all_records: list[dict] = []
        try:
            files = self._client.list(f"{self.BASE_DIR}/training_data")
        except Exception:
            return all_records

        for fname in files:
            try:
                fpath = f"{self.BASE_DIR}/training_data/{fname}"
                with self._client.read(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    all_records.extend(data)
                else:
                    all_records.append(data)
            except Exception:
                continue
        return all_records

    def save_model(self, model_bytes: bytes, version: str) -> str:
        """Save a serialized model to HDFS."""
        path = f"{self.BASE_DIR}/models/whale_model_{version}.pkl"
        with self._client.write(path, overwrite=True) as f:
            f.write(model_bytes)
        return path

    def write_report(self, report: dict, report_type: str) -> str:
        """Write a report (daily summary, backtest, etc.)."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = f"{self.BASE_DIR}/reports/{report_type}_{ts}.json"
        with self._client.write(path, encoding="utf-8", overwrite=True) as f:
            json.dump(report, f)
        return path
