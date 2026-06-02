import os
import time
import json
import random
import math
import hashlib
import signal
import sys

from datetime import datetime, timedelta
from pathlib import Path
from collections import deque
from zoneinfo import ZoneInfo
from typing import Optional

import pika
import paho.mqtt.client as mqtt


# =====================================
# TIMEZONE
# =====================================

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
UTC = ZoneInfo("UTC")


# =====================================
# LOGGER
# =====================================

import logging
from logging.handlers import RotatingFileHandler

LOG_FILE = Path("./smart_farm_simulator.log")

LOGGER = logging.getLogger("smart_farm_bridge")
LOGGER.setLevel(logging.INFO)

handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)

LOGGER.addHandler(handler)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


# =====================================
# CONFIG
# =====================================

STATION_CODE = os.getenv("STATION_CODE", "K13_TEST")

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq.hpcc.vn")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "smart_farm_data")
RABBIT_USER = os.getenv("RABBIT_USER", "smartfarm")
RABBIT_PASS = os.getenv("RABBIT_PASS", "9IAV441Wosw4dW")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt.abcsolutions.com.vn")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "abcsolution")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "CseLAbC5c6")
MQTT_TOPIC_PUB = os.getenv("MQTT_TOPIC_PUB", "/esp32Relay/request_test")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

NODE_ID = os.getenv("NODE_ID", "nha-anh-thin")

PUBLISH_INTERVAL = 10


# =====================================
# SENSOR STATE
# =====================================

sensor_state = {
    "temperature": 30.0,
    "humidity": 65.0,
    "soilPercent": 55.0,
    "lux": 2000.0,
    "rainValue": 3800
}


# =====================================
# ENVIRONMENT MODEL
# =====================================

def day_fraction(now):

    seconds = now.hour * 3600 + now.minute * 60 + now.second
    return seconds / 86400


def compute_lux(now):

    phase = day_fraction(now)

    # daylight sinus
    daylight = max(0, math.sin((phase - 0.25) * 2 * math.pi))

    lux = 200 + daylight * 8000

    noise = random.gauss(0, 50)

    return max(0, lux + noise)


def compute_temperature(lux):

    base = 26

    temp = base + lux / 1500

    temp += random.gauss(0, 0.2)

    return temp


def compute_humidity(temp):

    base = 80 - (temp - 25) * 1.5

    base += random.gauss(0, 0.5)

    return max(40, min(95, base))


def update_soil_moisture(soil, pump_on, lux):

    evaporation = lux / 100000

    soil -= evaporation

    if pump_on:
        soil += 0.5

    soil += random.gauss(0, 0.05)

    return max(20, min(90, soil))


def compute_rain():

    if random.random() < 0.005:
        return random.randint(0, 1500)

    return random.randint(3500, 4095)


# =====================================
# SENSOR GENERATOR
# =====================================

def generate_fake_sensor_data(pump_on):

    global sensor_state

    now = datetime.now(TZ)

    lux = compute_lux(now)

    temp = compute_temperature(lux)

    humidity = compute_humidity(temp)

    soil = update_soil_moisture(sensor_state["soilPercent"], pump_on, lux)

    rain = compute_rain()

    sensor_state.update({
        "temperature": temp,
        "humidity": humidity,
        "soilPercent": soil,
        "lux": lux,
        "rainValue": rain
    })

    return {
        "temperature": f"{temp:.2f}",
        "humidity": f"{humidity:.2f}",
        "soilPercent": f"{soil:.2f}",
        "lux": f"{lux:.2f}",
        "rainValue": rain
    }


# =====================================
# PUMP CONTROL
# =====================================

pump_on_until: Optional[datetime] = None
pump_state = 0

WATERING_SCHEDULE = {
    (6, 0): 60000,
    (12, 0): 90000,
    (18, 0): 60000,
}

last_triggered_day = {}


# =====================================
# DUPLICATE PROTECTION
# =====================================

_recent_hashes = deque(maxlen=1000)
_recent_set = set()


def remember(keyhash):

    if keyhash in _recent_set:
        return False

    _recent_hashes.append(keyhash)
    _recent_set.add(keyhash)

    if len(_recent_set) > 1000:
        _recent_set.clear()
        _recent_set.update(_recent_hashes)

    return True


# =====================================
# TELEMETRY FORMAT
# =====================================

def convert_to_telemetry(msg):

    telemetry = {
        "schema": "xanhmarket.telemetry.v1",
        "node_id": NODE_ID,
        "timestamp": datetime.now(UTC).isoformat(),
        "readings": [
            {"sensor": "AIR_TEMP_SENSOR", "type": "air_temp", "unit": "°C", "val": float(msg["temperature"])},
            {"sensor": "AIR_HUMIDITY_SENSOR", "type": "air_humidity", "unit": "%", "val": float(msg["humidity"])},
            {"sensor": "SOIL_MOISTURE_SENSOR", "type": "soil_moisture", "unit": "%", "val": float(msg["soilPercent"])},
            {"sensor": "LUX_SENSOR", "type": "lux", "unit": "lx", "val": float(msg["lux"])},
            {"sensor": "RAIN_SENSOR", "type": "rain", "unit": "raw", "val": int(msg["rainValue"])},
            {"sensor": "PUMP_RELAY", "channel": "pump_state", "unit": "bool", "val": pump_state}
        ]
    }

    return telemetry


# =====================================
# RABBITMQ
# =====================================

def publish_to_rabbit(message):

    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)

    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=creds
    )

    conn = pika.BlockingConnection(params)

    ch = conn.channel()

    ch.exchange_declare(
        exchange=RABBIT_EXCHANGE,
        exchange_type="fanout",
        durable=True
    )

    body = json.dumps(message)

    keyhash = hashlib.sha256(body.encode()).hexdigest()

    if not remember(keyhash):
        return

    ch.basic_publish(
        exchange=RABBIT_EXCHANGE,
        routing_key="",
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json"
        )
    )

    LOGGER.info("RabbitMQ Sent: %s", body)

    conn.close()


# =====================================
# MQTT PUMP COMMAND
# =====================================

def send_pump_command(duration_ms):

    payload = {
        "command": "PUMP_ON",
        "duration": duration_ms
    }

    client = mqtt.Client()

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_HOST, MQTT_PORT)

    client.publish(MQTT_TOPIC_PUB, json.dumps(payload))

    client.disconnect()

    LOGGER.info("MQTT pump command sent %s", payload)


# =====================================
# SIGNAL HANDLER
# =====================================

shutdown = False


def handle_signal(sig, frame):
    global shutdown
    shutdown = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# =====================================
# MAIN LOOP
# =====================================

def main():

    global pump_on_until
    global pump_state

    last_publish = None

    LOGGER.info("Smart Farm Simulator started")

    while not shutdown:

        now = datetime.now(TZ)

        hm = (now.hour, now.minute)

        if hm in WATERING_SCHEDULE:

            if last_triggered_day.get(hm) != now.date():

                duration = WATERING_SCHEDULE[hm]

                send_pump_command(duration)

                pump_state = 1
                pump_on_until = now + timedelta(milliseconds=duration)

                last_triggered_day[hm] = now.date()

        if pump_on_until and now >= pump_on_until:
            pump_state = 0

        if last_publish is None or (now - last_publish).total_seconds() >= PUBLISH_INTERVAL:

            sensor = generate_fake_sensor_data(pump_state)

            telemetry = convert_to_telemetry(sensor)

            publish_to_rabbit(telemetry)

            last_publish = now

        time.sleep(1)

    LOGGER.info("Service stopped")


# =====================================

if __name__ == "__main__":
    main()