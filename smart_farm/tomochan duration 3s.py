#!/usr/bin/env python3
"""
tomochan.py

- Subscribe to a raw MQTT topic (device payload)
- Convert incoming JSON to schema "xanhmarket.telemetry.v1"
- Send converted telemetry to RabbitMQ exchange (no publishing back to MQTT)
- Safe: uses a worker thread for RabbitMQ publishing (pika not called in MQTT callback)
- Logs the full body of each message published to RabbitMQ (printed & logged)
"""

from __future__ import annotations
import os
import json
import time
import logging
import traceback
import threading
import queue
from datetime import datetime, timezone
import sys
import random

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

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("tomochan")

# reduce noisy logs from pika / amqp libs
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("pika.adapters").setLevel(logging.WARNING)

# -------------------------
# Helpers (timestamp, safe casts)
# -------------------------
def iso_utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def parse_device_timestamp(payload: dict) -> str:
    # prefer device rtc field, else now (same as your original code)
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
# Conversion logic (unchanged)
# -------------------------
def convert_raw_to_telemetry(raw: dict) -> dict:
    telemetry = {
        "schema": "xanhmarket.telemetry.v1",
        "node_id": NODE_ID,
        "timestamp": parse_device_timestamp(raw),
        "readings": []
    }

    # # optional: keep station id
    # if "id" in raw:
    #     telemetry["station_code"] = raw.get("id")

    # DHT: respect valid flag
    dht = raw.get("dht")
    if isinstance(dht, dict) and dht.get("valid", True):
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
    else:
        # device marked invalid; we skip DHT readings
        logger.debug("Skipping DHT readings: valid flag false or missing")

    # Light
    light = raw.get("light")
    if isinstance(light, dict):
        lux = float_safe(light.get("intensity"))
        if lux is not None:
            # optional: clamp absurd lux values
            if lux < 0:
                logger.warning("Negative lux received: %s", lux)
            telemetry["readings"].append({
                "sensor": "MODBUS LUX SENSOR",
                "type": "lux",
                "unit": "lx",
                "val": lux
            })

    # Rain
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

    # TDS / EC / salinity
    tds = raw.get("tds")
    if isinstance(tds, dict) and tds.get("valid", True):
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

        # prefer device-provided temp if reasonable, else optional fallback
        tds_temp_device = float_safe(tds.get("temperature_c"))
        tds_temp = None
        if tds_temp_device is not None and -10.0 <= tds_temp_device <= 100.0:
            tds_temp = tds_temp_device
        else:
            # fallback policy: do NOT invent values in production.
            # If you *want* to generate a random debug value, set USE_RANDOM_TDS_TEMP=True
            USE_RANDOM_TDS_TEMP = False
            if USE_RANDOM_TDS_TEMP:
                tds_temp = random.uniform(28.0, 30.0)

        if tds_temp is not None:
            telemetry["readings"].append({
                "sensor": "TDS AND EC SENSOR",
                "type": "water_temp",
                "unit": "°C",
                "val": round(float(tds_temp), 2)
            })

    #     # include raw metadata like ec_raw
    #     if "ec_raw" in tds:
    #         telemetry.setdefault("meta", {})["ec_raw"] = tds.get("ec_raw")
    # else:
    #     logger.debug("Skipping TDS readings: valid flag false or missing")

    # # keep other metadata if present
    # if "reset" in raw:
    #     telemetry.setdefault("meta", {})["reset"] = raw.get("reset")

    return telemetry
# -------------------------
# RabbitMQ publisher worker (thread-safe)
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
                # ensure connection
                if not self._is_connected():
                    self._connect()
                # wait for next message (timeout so we can check stop flag)
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
        # clean up on stop
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
        # close old if exists
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
        # PRINT & LOG the full body being published
        try:
            # print to stdout (captured by systemd/redirected log)
            print(body)
        except Exception:
            pass
        logger.info("[RabbitMQ] Publishing body: %s", body)
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
        except Exception:
            pass
        try:
            if self._conn is not None and getattr(self._conn, "is_open", False):
                try:
                    self._conn.close()
                except Exception:
                    pass
        finally:
            self._channel = None
            self._conn = None

# -------------------------
# MQTT Bridge (only subscribe, do NOT publish back to MQTT)
# -------------------------
class Bridge:
    def __init__(self, publish_queue: "queue.Queue[dict]"):
        self.client = mqtt.Client(client_id=f"tomochan-bridge-{int(time.time())}")
        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        # IMPORTANT: do NOT set will_set that publishes to MQTT OUT_TOPIC/status
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self._connected = False
        self.publish_queue = publish_queue

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
                # DO NOT publish any status back to MQTT
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
            # send to rabbitmq via queue (non-blocking if possible)
            try:
                self.publish_queue.put_nowait(telemetry)
                logger.debug("Enqueued telemetry for RabbitMQ (node=%s ts=%s)", telemetry.get("node_id"), telemetry.get("timestamp"))
            except queue.Full:
                # queue full -> drop message with log (or you can block/wait)
                logger.error("Publish queue full; dropping telemetry (node=%s ts=%s)", telemetry.get("node_id"), telemetry.get("timestamp"))
        except Exception:
            logger.exception("on_message handler failed: %s", traceback.format_exc())

# -------------------------
# Main
# -------------------------
def main():
    publish_queue: "queue.Queue[dict]" = queue.Queue(maxsize=PUBLISH_QUEUE_MAX)
    rabbit_pub = RabbitPublisher(publish_queue)
    rabbit_pub.start()

    bridge = Bridge(publish_queue)
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

    logger.info("Bridge started: reading %s -> publishing to RabbitMQ exchange %s (node_id=%s).", SRC_TOPIC, RABBIT_EXCHANGE, NODE_ID)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    finally:
        # graceful stop: stop mqtt loop and rabbit worker
        try:
            bridge.disconnect()
        except Exception:
            pass
        rabbit_pub.stop()
        # Wait for worker to finish; join safely
        rabbit_pub.join(timeout=10)
        logger.info("Stopped")

if __name__ == "__main__":
    main()
