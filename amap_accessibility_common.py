#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

AMAP_POI_URL = "https://restapi.amap.com/v5/place/text"
AMAP_GEOCODE_URL = "https://restapi.amap.com/v3/geocode/geo"
AMAP_TRANSIT_URL = "https://restapi.amap.com/v5/direction/transit/integrated"
RETRIABLE_INFOS = {
    "CUQPS_HAS_EXCEEDED_THE_LIMIT",
    "DAILY_QUERY_OVER_LIMIT",
    "ACCESS_TOO_FREQUENT",
    "SYSTEM_ERROR",
}


@dataclass(frozen=True)
class RouteResult:
    from_id: str
    to_id: str
    status: str
    duration_seconds: Optional[int]
    transit_index: Optional[int]
    summary: str
    reason: str


@dataclass(frozen=True)
class AMapCredentialConfig:
    key: str
    secret: str


@dataclass
class AMapCredentialRuntime:
    key: str
    secret: str
    station_search_limiter: "AsyncQPSLimiter"
    route_plan_limiter: "AsyncQPSLimiter"


class AsyncQPSLimiter:
    def __init__(self, qps: float) -> None:
        if qps <= 0:
            raise ValueError("qps must be > 0")
        self.min_interval = 1.0 / qps
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            wait_seconds = max(0.0, self._next_allowed_at - now)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
                now = loop.time()
            self._next_allowed_at = now + self.min_interval


def default_service_date() -> str:
    today = date.today()
    if today.weekday() < 5:
        return today.isoformat()
    delta = 7 - today.weekday()
    return (today + timedelta(days=delta)).isoformat()


def load_env_file(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    merged = dict(env)
    for key, value in os.environ.items():
        if key not in merged:
            merged[key] = value
    return merged


def load_amap_credentials(env_values: Dict[str, str]) -> List[AMapCredentialConfig]:
    credentials: List[AMapCredentialConfig] = []

    key = env_values.get("KEY")
    secret = env_values.get("SEC")
    if key and secret:
        credentials.append(AMapCredentialConfig(key=key, secret=secret))

    indexed_keys: Dict[int, str] = {}
    indexed_secs: Dict[int, str] = {}
    for env_key, env_value in env_values.items():
        key_match = re.fullmatch(r"AMAP_KEY_(\d+)", env_key)
        if key_match:
            indexed_keys[int(key_match.group(1))] = env_value
            continue
        sec_match = re.fullmatch(r"AMAP_SEC_(\d+)", env_key)
        if sec_match:
            indexed_secs[int(sec_match.group(1))] = env_value

    for index in sorted(set(indexed_keys) | set(indexed_secs)):
        indexed_key = indexed_keys.get(index)
        indexed_sec = indexed_secs.get(index)
        if not indexed_key or not indexed_sec:
            raise RuntimeError(f"Missing AMAP_KEY_{index} or AMAP_SEC_{index}")
        credentials.append(AMapCredentialConfig(key=indexed_key, secret=indexed_sec))

    deduped: List[AMapCredentialConfig] = []
    seen_pairs: set[Tuple[str, str]] = set()
    for credential in credentials:
        pair = (credential.key, credential.secret)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        deduped.append(credential)

    if not deduped:
        raise RuntimeError(
            "Missing AMap credentials. Provide KEY/SEC or one or more AMAP_KEY_n/AMAP_SEC_n pairs in the environment"
        )
    return deduped


def normalize_text(value: str) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", value.lower())


def parse_duration_seconds(raw_value: Any) -> Optional[int]:
    if raw_value is None or raw_value == "":
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def summarize_transit(transit: Dict[str, Any]) -> str:
    labels: List[str] = []
    for segment in transit.get("segments") or []:
        for busline in ((segment.get("bus") or {}).get("buslines") or []):
            name = str(busline.get("name") or "").strip()
            if name:
                labels.append(name)
        railway = segment.get("railway") or {}
        railway_name = str(railway.get("name") or "").strip()
        if railway_name:
            labels.append(railway_name)
    return " | ".join(labels)


def transit_has_forbidden_mode(transit: Dict[str, Any]) -> Optional[str]:
    for segment in transit.get("segments") or []:
        if segment.get("taxi"):
            return "contains_taxi"
    return None


def route_result_is_final(result: Optional[RouteResult]) -> bool:
    if result is None:
        return False
    if result.status == "done":
        return True
    if result.status == "no_valid_route" and result.reason != "contains_maglev":
        return True
    return False


def select_transit(route_payload: Dict[str, Any], from_id: str, to_id: str) -> RouteResult:
    transits = (route_payload.get("route") or {}).get("transits") or []
    if not transits:
        return RouteResult(from_id, to_id, "no_valid_route", None, None, "", "no_transits")

    last_reason = "no_valid_transit"
    best_result: Optional[RouteResult] = None
    for index, transit in enumerate(transits):
        forbidden_reason = transit_has_forbidden_mode(transit)
        if forbidden_reason:
            last_reason = forbidden_reason
            continue
        duration_seconds = parse_duration_seconds((transit.get("cost") or {}).get("duration"))
        if duration_seconds is None:
            last_reason = "missing_duration"
            continue
        candidate = RouteResult(
            from_id=from_id,
            to_id=to_id,
            status="done",
            duration_seconds=duration_seconds,
            transit_index=index,
            summary=summarize_transit(transit),
            reason="ok",
        )
        if best_result is None or best_result.duration_seconds is None or duration_seconds < best_result.duration_seconds:
            best_result = candidate
    if best_result is not None:
        return best_result
    return RouteResult(from_id, to_id, "no_valid_route", None, None, "", last_reason)


class AMapClient:
    def __init__(
        self,
        credentials: Sequence[AMapCredentialConfig],
        pause_sec: float = 0.0,
        timeout_sec: int = 20,
        retries: int = 4,
        station_search_qps: float = 3.01,
        route_plan_qps: float = 3.01,
    ) -> None:
        self.pause_sec = pause_sec
        self.timeout_sec = timeout_sec
        self.retries = retries
        transport = httpx.AsyncHTTPTransport(
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30.0,
            ),
        )
        self.client = httpx.AsyncClient(timeout=self.timeout_sec, transport=transport)
        self.credentials = [
            AMapCredentialRuntime(
                key=credential.key,
                secret=credential.secret,
                station_search_limiter=AsyncQPSLimiter(station_search_qps),
                route_plan_limiter=AsyncQPSLimiter(route_plan_qps),
            )
            for credential in credentials
        ]
        self._station_search_lock = asyncio.Lock()
        self._route_plan_lock = asyncio.Lock()
        self._station_search_index = 0
        self._route_plan_index = 0

    async def aclose(self) -> None:
        await self.client.aclose()

    async def geocode(self, address: str, city: Optional[str] = None) -> List[Dict[str, Any]]:
        credential = await self._acquire_station_search_credential()
        params: Dict[str, Any] = {
            "address": address,
            "output": "JSON",
        }
        if city:
            params["city"] = city
        payload = await self._request_json(AMAP_GEOCODE_URL, params, credential)
        return list(payload.get("geocodes") or [])

    def sign_params(self, params: Dict[str, Any], credential: AMapCredentialRuntime) -> str:
        raw = "&".join(f"{key}={params[key]}" for key in sorted(params)) + credential.secret
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    async def _acquire_station_search_credential(self) -> AMapCredentialRuntime:
        async with self._station_search_lock:
            credential = self.credentials[self._station_search_index]
            self._station_search_index = (self._station_search_index + 1) % len(self.credentials)
        await credential.station_search_limiter.acquire()
        return credential

    async def _acquire_route_plan_credential(self) -> AMapCredentialRuntime:
        async with self._route_plan_lock:
            credential = self.credentials[self._route_plan_index]
            self._route_plan_index = (self._route_plan_index + 1) % len(self.credentials)
        await credential.route_plan_limiter.acquire()
        return credential

    async def _request_json(self, url: str, params: Dict[str, Any], credential: AMapCredentialRuntime) -> Dict[str, Any]:
        signed_params = {key: str(value) for key, value in params.items() if value is not None}
        signed_params["key"] = credential.key
        signed_params["sig"] = self.sign_params(signed_params, credential)

        last_error: Optional[str] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = await self.client.get(url, params=signed_params)
                response.raise_for_status()
                payload = response.json()
            except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
                last_error = str(exc)
                if attempt == self.retries:
                    raise RuntimeError(f"HTTP request failed after {self.retries} attempts: {url}; error={last_error}") from exc
                await asyncio.sleep(min(5.0, attempt * 0.8))
                continue

            status = str(payload.get("status") or "")
            info = str(payload.get("info") or "")
            if status == "1":
                if self.pause_sec > 0:
                    await asyncio.sleep(self.pause_sec)
                return payload

            last_error = f"{info} ({payload.get('infocode')})"
            if info in RETRIABLE_INFOS or "QPS" in info:
                await asyncio.sleep(min(8.0, attempt * 1.2))
                continue
            raise RuntimeError(f"AMap request failed: {url}; info={info}; infocode={payload.get('infocode')}")

        raise RuntimeError(f"AMap request failed after retries: {url}; error={last_error}")