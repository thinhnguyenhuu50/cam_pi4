import os
import time
import json
import re
import hashlib
import signal
import sys
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import deque
from zoneinfo import ZoneInfo
from typing import Optional

# Defensive imports for environment where packages may be missing
try:
    import requests
except Exception as e:
    raise RuntimeError("Missing required package 'requests'. Install with: pip install requests") from e

try:
    import pika
except Exception as e:
    raise RuntimeError("Missing required package 'pika'. Install with: pip install pika") from e

try:
    import paho.mqtt.client as mqtt
except Exception as e:
    raise RuntimeError("Missing required package 'paho-mqtt'. Install with: pip install paho-mqtt") from e

# bs4 is optional but preferred for XML-ish response parsing
try:
    from bs4 import BeautifulSoup  # type: ignore
    _HAS_BS4 = True
except Exception:
    _HAS_BS4 = False

# ============================
# 0) TIMEZONES & LOGGING
# ============================
TZ = ZoneInfo("Asia/Ho_Chi_Minh")
UTC = ZoneInfo("UTC")

# Logging: primary to /var/log, fallback to local file
import logging
from logging.handlers import RotatingFileHandler

LOG_FILE_PRIMARY = Path("/var/log/smart_farm_pump.log")
LOG_FILE_FALLBACK = Path("./smart_farm_pump.log")

LOGGER = logging.getLogger("smart_farm_bridge")
LOGGER.setLevel(logging.INFO)

def setup_logger():
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S%z")
    # try primary
    try:
        LOG_FILE_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(LOG_FILE_PRIMARY, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
        handler.setFormatter(fmt)
        LOGGER.addHandler(handler)
    except Exception:
        # fallback to local
        handler = RotatingFileHandler(LOG_FILE_FALLBACK, maxBytes=2*1024*1024, backupCount=2, encoding="utf-8")
        handler.setFormatter(fmt)
        LOGGER.addHandler(handler)

    # also log to stdout for container logs
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    LOGGER.addHandler(sh)

setup_logger()

def log_event(level: str, event: str, **kwargs):
    detail = " ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in kwargs.items())
    msg = f"[{event}] {detail}".strip()
    if level == "info":
        LOGGER.info(msg)
    elif level == "warning":
        LOGGER.warning(msg)
    elif level == "error":
        LOGGER.error(msg)
    else:
        LOGGER.debug(msg)

# ============================
# 1) ENV / STATIC CONFIG
# ============================
STATION_CODE = os.getenv("STATION_CODE", "K13_TEST")
EMS_API_URL = os.getenv(
    "EMS_API_URL",
    f"http://ems.thebestits.vn/api/Values/GetLatestData?station_code={STATION_CODE}",
)

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq.hpcc.vn")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "smart_farm_data")
RABBIT_USER = os.getenv("RABBIT_USER", "smartfarm")
RABBIT_PASS = os.getenv("RABBIT_PASS", "9IAV441Wosw4dW")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt.abcsolutions.com.vn")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "abcsolution")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "CseLAbC5c6")
MQTT_TOPIC_PUB = os.getenv("MQTT_TOPIC_PUB", "/esp32Relay/request_test")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))  # 1 = có PUBACK

NODE_ID = os.getenv("NODE_ID", "nha-anh-thin")  # node_id cho schema telemetry

LOOP_DELAY = float(os.getenv("LOOP_DELAY", "1"))
RECENT_MAX = int(os.getenv("RECENT_MAX", "1000"))

# Lịch tưới: (giờ, phút) -> thời lượng (ms)
WATERING_SCHEDULE = {
    (6, 0): 60000,
    (12, 0): 90000,
    (18, 0): 60000,
    (0, 0): 30000,
}

# HTTP fetch retry config
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_BACKOFF_SECONDS = float(os.getenv("HTTP_BACKOFF_SECONDS", "1.0"))

# ============================
# 2) STATE
# ============================
pump_on_until: Optional[datetime] = None   # timezone-aware
pump_state = 0         # 0 OFF, 1 ON
last_triggered_day: dict = {}  # {(h,m): date}

_recent_hashes = deque(maxlen=RECENT_MAX)
_recent_set = set()

def _remember(keyhash: str) -> bool:
    """True nếu chưa thấy; False nếu đã thấy. Tránh gửi trùng."""
    if keyhash in _recent_set:
        return False
    _recent_hashes.append(keyhash)
    _recent_set.add(keyhash)
    # keep set in sync with deque (prevent unlimited growth)
    if len(_recent_set) > _recent_hashes.maxlen:
        _recent_set.clear()
        _recent_set.update(_recent_hashes)
    return True

# ============================
# 3) UTILS
# ============================
def _parse_device_ts(record: dict) -> str:
    """
    Lấy timestamp thiết bị/EMS nếu có, fallback sang _bridge_ts, cuối cùng là now().
    Trả về ISO8601 UTC '...Z' with milliseconds.
    """
    candidates = [
        record.get("timestamp"),
        record.get("device_ts"),
        record.get("deviceTime"),
        record.get("_bridge_ts"),
    ]
    for v in candidates:
        if not v:
            continue
        try:
            # Accept strings like "2025-09-21T12:34:56Z" or with offset
            s = str(v)
            # Python's fromisoformat doesn't accept trailing Z, replace it
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except Exception:
            # try parsing epoch millis / seconds
            try:
                num = float(v)
                if num > 1e12:  # maybe micros? unlikely; skip
                    pass
                elif num > 1e9:
                    # milliseconds
                    dt = datetime.fromtimestamp(num / 1000.0, tz=UTC)
                    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                else:
                    # seconds
                    dt = datetime.fromtimestamp(num, tz=UTC)
                    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            except Exception:
                continue
    # fallback: now UTC
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _float_safe(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _int_safe(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

# ============================
# 4) MQTT – Gửi lệnh bật bơm
# ============================
def send_pump_command(duration_ms: int, reason: str = "schedule") -> bool:
    payload = {"command": "PUMP_ON", "duration": duration_ms}
    client = None
    try:
        client = mqtt.Client()
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        # short timeout socket-level
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        # use loop_start so publish won't hang the main thread on some network glitches
        client.loop_start()
        info = client.publish(MQTT_TOPIC_PUB, json.dumps(payload), qos=MQTT_QOS, retain=False)
        # wait but with small timeout to avoid hanging forever
        info.wait_for_publish(timeout=5)
        log_event("info", "TOGGLE", topic=MQTT_TOPIC_PUB, payload=payload, reason=reason)
        LOGGER.info("[MQTT] Sent pump command: %s", payload)
        return True
    except Exception as e:
        LOGGER.exception("[MQTT] Send failed")
        log_event("error", "mqtt_publish_failed", message=str(e), payload=payload)
        return False
    finally:
        try:
            if client:
                client.loop_stop()
                client.disconnect()
        except Exception:
            pass

# ============================
# 5) EMS FETCH (with retries)
# ============================
def _extract_json_from_body(raw_text: str) -> str:
    """
    Try to extract JSON from response. Prefer bs4 <string> tag if available,
    else regex fallback that finds first top-level {...}.
    """
    if _HAS_BS4:
        try:
            soup = BeautifulSoup(raw_text, "html.parser")
            tag = soup.find("string")
            if tag and tag.text:
                return tag.text.strip()
        except Exception:
            # fall through to regex
            pass

    m = re.search(r'(\{.*\})', raw_text, re.DOTALL)
    if not m:
        raise ValueError("Không trích được JSON từ response")
    return m.group(1)

def fetch_ems_data():
    """
    Fetch EMS API with retries/backoff. Returns parsed dict, adds station_code and _bridge_ts.
    May raise exceptions for persistent failures which caller should handle.
    """
    global pump_on_until, pump_state

    last_exc = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.get(EMS_API_URL, timeout=5)
            resp.raise_for_status()
            raw = resp.text.strip()
            json_str = _extract_json_from_body(raw)

            # sometimes JSON is double-quoted with escaped quotes
            json_str = json_str.replace('\\"', '"')
            if json_str.startswith('"') and json_str.endswith('"'):
                json_str = json_str[1:-1]

            data = json.loads(json_str)
            data['station_code'] = STATION_CODE

            # Update pump state according to time
            now = datetime.now(TZ)
            new_state = 1 if pump_on_until and now < pump_on_until else 0
            if new_state != pump_state:
                # update global pump_state, log event
                # note: do not mutate pump_state until callers coordinate
                pass

            data.setdefault('_bridge_ts', datetime.now(TZ).isoformat(timespec="milliseconds"))
            return data
        except Exception as e:
            last_exc = e
            LOGGER.warning("fetch_ems_data attempt %d failed: %s", attempt, str(e))
            time.sleep(HTTP_BACKOFF_SECONDS * attempt)
    # all attempts failed
    LOGGER.exception("fetch_ems_data failed after retries")
    raise last_exc

# ============================
# 6) MAPPING → xanhmarket.telemetry.v1
# ============================
def convert_ems_to_telemetry(msg: dict) -> dict:
    telemetry = {
        "schema": "xanhmarket.telemetry.v1",
        "node_id": NODE_ID,
        "timestamp": _parse_device_ts(msg),
        "readings": []
    }

    # Nhiệt độ không khí
    if "temperature" in msg:
        v = _float_safe(msg.get("temperature"))
        if v is not None:
            telemetry["readings"].append({
                "sensor": "AIR_TEMP_SENSOR",
                "type": "air_temp",
                "unit": "°C",
                "val": v
            })

    # Độ ẩm không khí
    if "humidity" in msg:
        v = _float_safe(msg.get("humidity"))
        if v is not None:
            telemetry["readings"].append({
                "sensor": "AIR_HUMIDITY_SENSOR",
                "type": "air_humidity",
                "unit": "%",
                "val": v
            })

    # Độ ẩm đất
    if "soilPercent" in msg:
        v = _float_safe(msg.get("soilPercent"))
        if v is not None:
            telemetry["readings"].append({
                "sensor": "SOIL_MOISTURE_SENSOR",
                "type": "soil_moisture",
                "unit": "%",
                "val": v
            })

    # Ánh sáng
    if "lux" in msg:
        v = _float_safe(msg.get("lux"))
        if v is not None:
            telemetry["readings"].append({
                "sensor": "LUX_SENSOR",
                "type": "lux",
                "unit": "lx",
                "val": v
            })

    # Mưa
    if "rainValue" in msg:
        raw_adc = _int_safe(msg.get("rainValue"))
        if raw_adc is not None:
            telemetry["readings"].append({
                "sensor": "RAIN_SENSOR",
                "type": "rain",
                "unit": "raw",
                "val": raw_adc,
                "raw": {"adc": raw_adc}
            })

    # Trạng thái bơm
    telemetry["readings"].append({
        "sensor": "PUMP_RELAY",
        "channel": "pump_state",
        "unit": "bool",
        "val": int(msg.get("pump", 0))
    })

    return telemetry

# ============================
# 7) PUBLISH RABBITMQ (telemetry format)
# ============================
def publish_to_rabbit(messages):
    """
    messages: list[dict] – raw EMS records
    -> convert -> publish theo schema xanhmarket.telemetry.v1
    This function is robust: it attempts to connect/publish, logs errors per-message and continues.
    """
    if not messages:
        return

    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=15,
        connection_attempts=3,
        retry_delay=2,
        socket_timeout=5
    )

    conn = None
    try:
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type='fanout', durable=True)

        for msg in messages:
            try:
                formatted = convert_ems_to_telemetry(msg)
                body = json.dumps(formatted, ensure_ascii=False)
                keyhash = hashlib.sha256(body.encode("utf-8")).hexdigest()
                if not _remember(keyhash):
                    LOGGER.debug("Skipping duplicate payload")
                    continue

                ch.basic_publish(
                    exchange=RABBIT_EXCHANGE,
                    routing_key='',
                    body=body.encode('utf-8'),
                    properties=pika.BasicProperties(
                        content_type='application/json',
                        delivery_mode=2
                    )
                )
                LOGGER.info("[RabbitMQ] Sent → %s", body)
            except Exception:
                LOGGER.exception("Failed to publish one message; continuing")
    except Exception:
        LOGGER.exception("RabbitMQ connection/publish failed")
        raise
    finally:
        try:
            if conn and conn.is_open:
                conn.close()
        except Exception:
            pass

# ============================
# 8) MAIN LOOP with graceful shutdown
# ============================
_shutdown_requested = False

def _signal_handler(signum, frame):
    global _shutdown_requested
    LOGGER.info("Signal %s received, shutting down...", signum)
    _shutdown_requested = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def main():
    global pump_on_until, pump_state

    log_event("info", "SERVICE_START", message="SmartFarm bridge started")
    last_publish = None

    try:
        while not _shutdown_requested:
            now = datetime.now(TZ)
            hour_min = (now.hour, now.minute)

            # 8.1 Lịch tưới: chỉ 1 lần/slot/ngày
            if hour_min in WATERING_SCHEDULE:
                last_day = last_triggered_day.get(hour_min)
                # if not triggered today AND pump not currently scheduled
                if last_day != now.date() and (not pump_on_until or now >= pump_on_until):
                    duration_ms = WATERING_SCHEDULE[hour_min]
                    ok = send_pump_command(duration_ms, reason=f"schedule_{hour_min[0]:02d}:{hour_min[1]:02d}")
                    if ok:
                        pump_on_until = now + timedelta(milliseconds=duration_ms)
                        last_triggered_day[hour_min] = now.date()
                        if pump_state == 0:
                            pump_state = 1
                            log_event("info", "PUMP_ON", duration_ms=duration_ms,
                                      until=pump_on_until.isoformat(), reason="schedule_start")
                    else:
                        log_event("warning", "PUMP_COMMAND_FAILED", duration_ms=duration_ms, reason="schedule")

            # 8.2 Fetch EMS -> publish telemetry mỗi 60 giây
            if last_publish is None or (now - last_publish).total_seconds() >= 60:
                try:
                    record = fetch_ems_data()
                    # Ensure pump field reflects current scheduled state
                    record['pump'] = int(pump_state)
                    publish_to_rabbit([record])
                except Exception as e:
                    LOGGER.exception("Error fetching or publishing")
                    log_event("error", "fetch_or_publish", message=str(e))
                last_publish = now

            # 8.3 Guard OFF nếu quá hạn
            if pump_on_until and datetime.now(TZ) >= pump_on_until and pump_state == 1:
                pump_state = 0
                log_event("info", "PUMP_OFF", reason="duration_expired_guard")

            # small sleep to avoid busy-looping; keep responsive to schedule/minute ticks
            for _ in range(int(max(1, LOOP_DELAY))):
                if _shutdown_requested:
                    break
                time.sleep(1)
    except Exception:
        LOGGER.exception("Unhandled exception in main loop")
    finally:
        log_event("info", "SERVICE_STOP", message="SmartFarm bridge stopped")
        LOGGER.info("Shutdown complete.")

if __name__ == '__main__':
    main()
