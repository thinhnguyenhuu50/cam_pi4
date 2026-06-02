#!/usr/bin/env python3
"""
tomochan.py

- Subscribe to a raw MQTT topic (device payload)
- Convert incoming JSON to schema "xanhmarket.telemetry.v1"
- Aggregate readings continuously and publish averaged telemetry to RabbitMQ every PUBLISH_INTERVAL seconds
- Does NOT publish back to MQTT
"""

from __future__ import annotations
import os
import json
import time
import logging
import traceback
import threading
import queue
import random
from datetime import datetime, timezone
import sys

try:
    import paho.mqtt.client as mqtt
except Exception as e:
    raise RuntimeError("Missing dependency 'paho-mqtt'. Install with: pip install paho-mqtt") from e

try:
    import pika
except Exception as e:
    raise RuntimeError("Missing dependency 'pika'. Install with: pip install pika") from e

# -------------------------
# CONFIG (env overrides allowed)
# -------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "mqtt.abcsolutions.com.vn")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "abcsolution")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "CseLAbC5c6")

# topic where raw device publishes (subscribe)
SRC_TOPIC = os.getenv("SRC_TOPIC", "young/smartFarm/publish_test")

# node id to inject
NODE_ID = os.getenv("NODE_ID", "tomochan")

# QoS for subscribe (we don't publish to MQTT anymore)
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

# RabbitMQ config
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq.hpcc.vn")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "smartfarm")
RABBIT_PASS = os.getenv("RABBIT_PASS", "9IAV441Wosw4dW")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "smart_farm_data")
RABBIT_EXCHANGE_TYPE = os.getenv("RABBIT_EXCHANGE_TYPE", "fanout")  # fanout by default

# Worker / queue sizing
PUBLISH_QUEUE_MAX = int(os.getenv("PUBLISH_QUEUE_MAX", "1000"))

# Publish interval in seconds (aggregate window). Default 60s.
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL", "60"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("tomochan")

# quiet pika logs
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("pika.adapters").setLevel(logging.WARNING)

# -------------------------
# Helpers (timestamp, safe casts)
# -------------------------
def iso_utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def parse_device_timestamp(payload: dict) -> str:
    # prefer device rtc field, else now
    for key in ("rtc", "timestamp", "device_ts", "_bridge_ts"):
        v = payload.get(key)
        if v:
            try:
                s = str(v)
                if s.isdigit():
                    num = int(s)
                    if num > 1_000_000_000_000:  # millis
                        ts = datetime.fromtimestamp(num / 1000.0, tz=timezone.utc)
                    else:
                        ts = datetime.fromtimestamp(num, tz=timezone.utc)
                    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                else:
                    s2 = s.replace("Z", "+00:00")
                    try:
                        dt = datetime.fromisoformat(s2)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    except Exception:
                        continue
            except Exception:
                continue
    return iso_utc_now()

def float_safe(v):
    try:
        return float(v)
    except Exception:
        return None

def int_safe(v):
    try:
        return int(v)
    except Exception:
        return None

# -------------------------
# Conversion logic (kept close to your version)
# -------------------------
def convert_raw_to_telemetry(raw: dict) -> dict:
    """
    Convert a single incoming raw device JSON into the telemetry dict format.
    This function is unchanged except it still uses random for TDS temp if needed.
    """
    telemetry = {
        "schema": "xanhmarket.telemetry.v1",
        "node_id": NODE_ID,
        "timestamp": parse_device_timestamp(raw),
        "readings": []
    }

    # if "id" in raw:
    #     telemetry["station_code"] = raw.get("id")

    dht = raw.get("dht")
    if isinstance(dht, dict):
        # NOTE: you asked to keep random temperature behavior later; here we keep original mapping
        t = float_safe(dht.get("temperature_c"))
        h = float_safe(dht.get("humidity_pct"))
        if t is not None:
            telemetry["readings"].append({
                "sensor": "SHTC3",
                "type": "air_temp",
                "unit": "°C",
                "val": t
            })
        if h is not None:
            telemetry["readings"].append({
                "sensor": "SHTC3",
                "type": "air_humidity",
                "unit": "%",
                "val": h
            })

    light = raw.get("light")
    if isinstance(light, dict):
        lux = float_safe(light.get("intensity"))
        if lux is not None:
            telemetry["readings"].append({
                "sensor": "MODBUS LUX SENSOR",
                "type": "lux",
                "unit": "lx",
                "val": lux
            })

    rain = raw.get("rain")
    if isinstance(rain, dict):
        status = rain.get("status")
        if status is not None:
            telemetry["readings"].append({
                "sensor": "MODBUS RAIN SENSOR",
                "type": "rain",
                "unit": "raw",
                "val": int_safe(status) if status is not None else status,
                "raw": {
                    k: v for k, v in {
                        "lower_limit": rain.get("lower_limit"),
                        "hysteresis": rain.get("hysteresis"),
                        "reset_delay": rain.get("reset_delay"),
                        "sensitivity": rain.get("sensitivity")
                    }.items() if v is not None
                } or None
            })

    tds = raw.get("tds")
    if isinstance(tds, dict):
        tds_val = float_safe(tds.get("tds"))
        if tds_val is not None:
            telemetry["readings"].append({
                "sensor": "TDS AND EC SENSOR",
                "type": "tds",
                "unit": "ppm",
                "val": tds_val
            })
        ec = float_safe(tds.get("ec"))
        if ec is not None:
            telemetry["readings"].append({
                "sensor": "TDS AND EC SENSOR",
                "type": "ec",
                "unit": "mS/cm",
                "val": ec
            })
        sal = float_safe(tds.get("salinity"))
        if sal is not None:
            telemetry["readings"].append({
                "sensor": "TDS AND EC SENSOR",
                "type": "salinity",
                "unit": "ppt",
                "val": sal
            })

        # keep random behavior if device temp is absent or you specifically want random
        tds_temp = float_safe(tds.get("temperature_c"))
        # if tds_temp is None:
            # preserve your request: keep random temp generation
        tds_temp = float_safe(f"{random.uniform(28.0, 30.0):.2f}")
        # if tds_temp is not None:
        telemetry["readings"].append({
            "sensor": "TDS AND EC SENSOR",
            "type": "water_temp",
            "unit": "°C",
            "val": tds_temp
        })

        # if "ec_raw" in tds:
        #     telemetry.setdefault("meta", {})["ec_raw"] = tds.get("ec_raw")

    # # forward reset if present
    # if "reset" in raw:
    #     telemetry.setdefault("meta", {})["reset"] = raw.get("reset")

    return telemetry

# -------------------------
# Aggregator: accumulate numeric sums & counts, keep last for non-numeric
# -------------------------
class Aggregator:
    """
    Thread-safe aggregator that collects telemetry readings per (node_id, station_code).
    For numeric readings: keeps sum/count for averaging.
    For non-numeric or 'raw' subfields: keeps last-seen.
    """
    def __init__(self):
        # structure: {(node_id, station_code): { key -> {sum, count, unit, sensor, type, last_raw, last_seen} } }
        self.lock = threading.Lock()
        self.data = {}  # dict key -> per-sensor dict

    def _bucket_key(self, telemetry: dict):
        node = telemetry.get("node_id", NODE_ID)
        station = telemetry.get("station_code")
        return (node, station)

    def add(self, telemetry: dict):
        key = self._bucket_key(telemetry)
        with self.lock:
            bucket = self.data.setdefault(key, {})
            for r in telemetry.get("readings", []):
                # create key that groups same sensor/type/unit
                sensor = r.get("sensor")
                rtype = r.get("type")
                unit = r.get("unit")
                # unique id for reading slot
                rid = f"{sensor}||{rtype}||{unit}"
                val = r.get("val")
                # numeric?
                if isinstance(val, (int, float)) or (isinstance(val, str) and val.replace('.', '', 1).isdigit()):
                    try:
                        vnum = float(val)
                    except Exception:
                        vnum = None
                    if vnum is not None:
                        entry = bucket.setdefault(rid, {"sum": 0.0, "count": 0, "sensor": sensor, "type": rtype, "unit": unit, "last_raw": None, "last_seen": time.time()})
                        entry["sum"] += vnum
                        entry["count"] += 1
                        entry["last_seen"] = time.time()
                        # keep raw subdict if present
                        if isinstance(r.get("raw"), dict):
                            entry["last_raw"] = r.get("raw")
                else:
                    # non-numeric: keep last
                    entry = bucket.setdefault(rid, {"sum": 0.0, "count": 0, "sensor": sensor, "type": rtype, "unit": unit, "last_raw": None, "last_seen": time.time(), "last_val": None})
                    entry["last_val"] = val
                    entry["last_seen"] = time.time()
                    if isinstance(r.get("raw"), dict):
                        entry["last_raw"] = r.get("raw")
            # also capture top-level meta if present (keep last)
            if "meta" in telemetry:
                meta_bucket = bucket.setdefault("_meta", {})
                meta_bucket.update(telemetry.get("meta", {}))
            self.data[key] = bucket

    def flush(self) -> list[dict]:
        """
        Compute averaged telemetry for each bucket and reset aggregator.
        Returns list of telemetry dicts ready to publish.
        """
        now_ts = iso_utc_now()
        out = []
        with self.lock:
            items = list(self.data.items())
            self.data.clear()
        for (node, station), bucket in items:
            # if bucket empty skip
            if not bucket:
                continue
            telemetry = {"schema": "xanhmarket.telemetry.v1", "node_id": node or NODE_ID, "timestamp": now_ts, "readings": []}
            if station:
                telemetry["station_code"] = station
            # include meta first if present
            if "_meta" in bucket:
                telemetry.setdefault("meta", {}).update(bucket["_meta"])
            for rid, entry in bucket.items():
                if rid == "_meta":
                    continue
                sensor = entry.get("sensor")
                rtype = entry.get("type")
                unit = entry.get("unit")
                if entry.get("count", 0) > 0:
                    avg = entry["sum"] / float(entry["count"])
                    # round to 2 decimals when appropriate
                    # keep integers if average is whole number
                    val = round(avg, 2)
                    # if unit is bool-like, convert to int 0/1 by rounding
                    if unit == "bool":
                        val = int(round(avg))
                    reading = {"sensor": sensor, "type": rtype, "unit": unit, "val": val}
                    if entry.get("last_raw") is not None:
                        reading["raw"] = entry.get("last_raw")
                    telemetry["readings"].append(reading)
                else:
                    # non-numeric last_val
                    last_val = entry.get("last_val")
                    reading = {"sensor": sensor, "type": rtype, "unit": unit, "val": last_val}
                    if entry.get("last_raw") is not None:
                        reading["raw"] = entry.get("last_raw")
                    telemetry["readings"].append(reading)
            out.append(telemetry)
        return out

# -------------------------
# RabbitMQ publisher worker (thread-safe) - unchanged
# -------------------------
class RabbitPublisher(threading.Thread):
    def __init__(self, publish_queue: "queue.Queue[dict]"):
        super().__init__(daemon=True)
        self.queue = publish_queue
        self._stop_event = threading.Event()
        self._conn = None
        self._channel = None

    def run(self):
        retry = 0
        while not self._stop_event.is_set():
            try:
                if not self._is_connected():
                    self._connect()
                try:
                    item = self.queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                try:
                    self._publish_item(item)
                except Exception:
                    logger.exception("Failed to publish item; requeueing")
                    try:
                        self.queue.put(item, block=False)
                    except Exception:
                        logger.error("Publish queue full; dropping message")
                finally:
                    self.queue.task_done()
                retry = 0
            except Exception:
                logger.exception("RabbitPublisher encountered error; will retry connect")
                retry += 1
                backoff = min(30, 2 ** min(retry, 6))
                time.sleep(backoff)
        self._close()

    def stop(self):
        self._stop_event.set()

    def _is_connected(self) -> bool:
        return self._conn is not None and getattr(self._conn, "is_open", False) and self._channel is not None and getattr(self._channel, "is_open", False)

    def _connect(self):
        logger.info("Connecting to RabbitMQ %s:%d vhost=%s ...", RABBIT_HOST, RABBIT_PORT, RABBIT_VHOST)
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
        try:
            self._close()
        except Exception:
            pass
        self._conn = pika.BlockingConnection(params)
        self._channel = self._conn.channel()
        self._channel.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type=RABBIT_EXCHANGE_TYPE, durable=True)
        logger.info("RabbitMQ connected and exchange declared: %s (%s)", RABBIT_EXCHANGE, RABBIT_EXCHANGE_TYPE)

    def _publish_item(self, item: dict):
        if not self._is_connected():
            raise RuntimeError("Not connected to RabbitMQ")
        body = json.dumps(item, ensure_ascii=False)
        # print to stdout (captured by systemd or logs)
        try:
            print(body)
        except Exception:
            pass
        logger.info("[RabbitMQ] Publishing body (len=%d) node=%s ts=%s", len(body), item.get("node_id"), item.get("timestamp"))
        self._channel.basic_publish(
            exchange=RABBIT_EXCHANGE,
            routing_key="",
            body=body.encode("utf-8"),
            properties=pika.BasicProperties(content_type='application/json', delivery_mode=2)
        )
        logger.info("[RabbitMQ] Published message (node=%s ts=%s) len=%d", item.get("node_id"), item.get("timestamp"), len(body))

    def _close(self):
        try:
            if self._channel is not None and getattr(self._channel, "is_open", False):
                try:
                    self._channel.close()
                except Exception:
                    pass
            if self._conn is not None and getattr(self._conn, "is_open", False):
                try:
                    self._conn.close()
                except Exception:
                    pass
        finally:
            self._channel = None
            self._conn = None

# -------------------------
# Flusher thread: every PUBLISH_INTERVAL seconds compute averages and enqueue
# -------------------------
class Flusher(threading.Thread):
    def __init__(self, aggregator: Aggregator, publish_queue: "queue.Queue[dict]"):
        super().__init__(daemon=True)
        self.aggregator = aggregator
        self.queue = publish_queue
        self._stop_event = threading.Event()

    def run(self):
        logger.info("Flusher started: publishing aggregated telemetry every %ds", PUBLISH_INTERVAL)
        while not self._stop_event.is_set():
            # sleep in small increments to be responsive to shutdown
            waited = 0.0
            interval = PUBLISH_INTERVAL
            while waited < interval and not self._stop_event.is_set():
                time.sleep(0.5)
                waited += 0.5
            if self._stop_event.is_set():
                break
            try:
                messages = self.aggregator.flush()
                for m in messages:
                    try:
                        self.queue.put_nowait(m)
                        logger.debug("Enqueued aggregated telemetry for publish: node=%s ts=%s", m.get("node_id"), m.get("timestamp"))
                    except queue.Full:
                        logger.error("Publish queue full; dropping aggregated telemetry node=%s ts=%s", m.get("node_id"), m.get("timestamp"))
            except Exception:
                logger.exception("Flusher encountered error during flush")
        logger.info("Flusher stopped")

    def stop(self):
        self._stop_event.set()

# -------------------------
# MQTT Bridge (only subscribe, aggregate, do NOT publish back to MQTT)
# -------------------------
class Bridge:
    def __init__(self, aggregator: Aggregator):
        # use callback api default; it's fine
        self.client = mqtt.Client(client_id=f"tomochan-bridge-{int(time.time())}")
        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        # do NOT set will_set that publishes to MQTT OUT_TOPIC/status
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self._connected = False
        self.aggregator = aggregator

    def connect(self):
        logger.info("Connecting to MQTT %s:%d ...", MQTT_HOST, MQTT_PORT)
        try:
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            self.client.loop_start()
        except Exception:
            logger.exception("Failed to connect to MQTT broker")
            raise

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT broker (rc=%s)", rc)
            self._connected = True
            try:
                client.subscribe(SRC_TOPIC, qos=MQTT_QOS)
                logger.info("Subscribed to source topic: %s", SRC_TOPIC)
            except Exception:
                logger.exception("Subscription failed")
        else:
            logger.warning("MQTT connect returned rc=%s", rc)

    def on_disconnect(self, client, userdata, rc):
        self._connected = False
        logger.warning("MQTT disconnected (rc=%s). Will attempt reconnect.", rc)

    def on_message(self, client, userdata, msg):
        try:
            payload_raw = msg.payload.decode("utf-8", errors="ignore")
            logger.debug("Received message on %s: %s", msg.topic, payload_raw)
            try:
                obj = json.loads(payload_raw)
            except Exception:
                logger.exception("Invalid JSON received; ignoring")
                return

            telemetry = convert_raw_to_telemetry(obj)
            # Add telemetry into aggregator (non-blocking)
            try:
                self.aggregator.add(telemetry)
            except Exception:
                logger.exception("Failed to add telemetry to aggregator")
        except Exception:
            logger.exception("on_message handler failed: %s", traceback.format_exc())

# -------------------------
# Main
# -------------------------
def main():
    publish_queue: "queue.Queue[dict]" = queue.Queue(maxsize=PUBLISH_QUEUE_MAX)
    rabbit_pub = RabbitPublisher(publish_queue)
    rabbit_pub.start()

    aggregator = Aggregator()
    flusher = Flusher(aggregator, publish_queue)
    flusher.start()

    bridge = Bridge(aggregator)
    # connect MQTT with retry loop
    retry = 0
    while True:
        try:
            bridge.connect()
            break
        except Exception:
            retry += 1
            wait = min(30, 2 ** min(retry, 6))
            logger.warning("MQTT connect failed, retrying in %ds...", wait)
            time.sleep(wait)

    logger.info("Bridge started: reading %s -> publishing aggregated telemetry to RabbitMQ exchange %s every %ds (node_id=%s).",
                SRC_TOPIC, RABBIT_EXCHANGE, PUBLISH_INTERVAL, NODE_ID)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    finally:
        try:
            bridge.disconnect()
        except Exception:
            pass
        flusher.stop()
        rabbit_pub.stop()
        # join threads safely
        flusher.join(timeout=5)
        rabbit_pub.join(timeout=10)
        logger.info("Stopped")

if __name__ == "__main__":
    main()
