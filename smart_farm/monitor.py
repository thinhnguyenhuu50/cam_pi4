
from __future__ import annotations
import os
import sys
import time
import json
import logging
import signal
from datetime import datetime, timezone

try:
    import pika
except Exception:
    print("Missing dependency 'pika'. Install with: pip install pika", file=sys.stderr)
    raise

# Config (defaults match your tomocha setup)
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq.hpcc.vn")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "smartfarm")
RABBIT_PASS = os.getenv("RABBIT_PASS", "9IAV441Wosw4dW")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "smart_farm_data")
RABBIT_EXCHANGE_TYPE = os.getenv("RABBIT_EXCHANGE_TYPE", "fanout")  # fanout/direct/topic
# Optional: routing_key to bind (ignored for fanout)
BIND_ROUTING_KEY = os.getenv("BIND_ROUTING_KEY", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("rabbit-monitor")

_stop = False
def _signal_handler(signum, frame):
    global _stop
    logger.info("Signal %s received, stopping...", signum)
    _stop = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def pretty_print_body(body_bytes: bytes, max_len=2000):
    try:
        s = body_bytes.decode("utf-8")
    except Exception:
        print(f"[{datetime.now(timezone.utc).isoformat()}] <binary payload {len(body_bytes)} bytes>")
        return

    if len(s) > max_len:
        s_short = s[:max_len] + "...[truncated]"
    else:
        s_short = s

    try:
        obj = json.loads(s_short)
        pretty = json.dumps(obj, ensure_ascii=False, indent=2)
        print(f"\n[{datetime.now(timezone.utc).isoformat()}] MESSAGE (len={len(body_bytes)})\n{pretty}\n")
    except Exception:
        print(f"\n[{datetime.now(timezone.utc).isoformat()}] MESSAGE (len={len(body_bytes)})\n{s_short}\n")

def ensure_exchange(channel):
    """
    Ensure exchange exists without changing its durable flag if it already exists.
    - Try passive declare (check only).
    - If not found, declare durable=True.
    """
    try:
        channel.exchange_declare(exchange=RABBIT_EXCHANGE, passive=True)
        logger.info("Exchange '%s' exists (passive check).", RABBIT_EXCHANGE)
    except pika.exceptions.ChannelClosedByBroker as e:
        # Channel closed - usually means exchange not found (404) or other precondition
        # Re-open channel and declare durable=True
        logger.info("Exchange '%s' not present or precondition failed: %s. Will declare durable=True.", RABBIT_EXCHANGE, e)
        # Need to re-open channel on caller side (raising to allow caller to re-get connection)
        raise

def monitor_loop():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=15,
    )

    while not _stop:
        conn = None
        ch = None
        try:
            logger.info("Connecting to RabbitMQ %s:%d vhost=%s ...", RABBIT_HOST, RABBIT_PORT, RABBIT_VHOST)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()

            # First attempt passive declare; if that results in ChannelClosedByBroker we will create durable exchange
            try:
                ch.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type=RABBIT_EXCHANGE_TYPE, passive=True)
                logger.info("Exchange '%s' exists (passive).", RABBIT_EXCHANGE)
            except pika.exceptions.ChannelClosedByBroker as e:
                # channel closed by broker - reopen connection/channel and declare durable
                logger.warning("Passive declare failed (%s). Recreating channel and declaring durable exchange.", e)
                try:
                    # reopen connection/channel
                    try:
                        ch.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                except Exception:
                    pass
                # reopen fresh
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type=RABBIT_EXCHANGE_TYPE, durable=True)
                logger.info("Declared durable exchange '%s'.", RABBIT_EXCHANGE)

            # create exclusive, autodelete queue with random name
            result = ch.queue_declare(queue='', exclusive=True, auto_delete=True)
            queue_name = result.method.queue
            logger.info("Declared exclusive queue: %s", queue_name)

            # bind to exchange; for fanout routing_key ignored
            ch.queue_bind(exchange=RABBIT_EXCHANGE, queue=queue_name, routing_key=BIND_ROUTING_KEY)
            logger.info("Bound queue %s to exchange %s (type=%s) with routing_key='%s'", queue_name, RABBIT_EXCHANGE, RABBIT_EXCHANGE_TYPE, BIND_ROUTING_KEY)

            def on_message(ch_local, method, properties, body):
                hdr = properties.headers if properties and properties.headers else {}
                print(f"[{datetime.now(timezone.utc).isoformat()}] Received. exchange={method.exchange} routing_key={method.routing_key} delivery_tag={method.delivery_tag} headers={hdr}")
                pretty_print_body(body)
                try:
                    ch_local.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    pass

            logger.info("Starting consume... (Ctrl-C to stop)")
            ch.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)

            while not _stop and conn.is_open:
                conn.process_data_events(time_limit=1.0)

            try:
                ch.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            if _stop:
                break

        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("Connection error: %s -- retrying in 5s", e)
            time.sleep(5)
        except pika.exceptions.ChannelClosedByBroker as e:
            # This can happen if exchange exists but flags mismatch (handled above) or other preconditions.
            logger.warning("Channel closed by broker: %s -- retrying in 5s", e)
            time.sleep(5)
        except Exception as e:
            logger.exception("Unexpected error: %s -- retrying in 5s", e)
            time.sleep(5)
        finally:
            try:
                if ch and getattr(ch, "is_open", False):
                    try:
                        ch.close()
                    except Exception:
                        pass
                if conn and getattr(conn, "is_open", False):
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                pass

    logger.info("Monitor exiting.")

if __name__ == "__main__":
    print("RabbitMQ monitor - listening for messages on exchange:", RABBIT_EXCHANGE)
    monitor_loop()
