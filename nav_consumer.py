import psycopg2
import pika
import xml.etree.ElementTree as ET
import signal
import sys
import os       # <-- Добавили
import json     # <-- Добавили
from datetime import datetime
import logging

# ===================== ЛОГИ =====================
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# ===================== ЧТЕНИЕ APPSETTINGS =====================
if getattr(sys, 'frozen', False):
    base_dir = os.path.dirname(sys.executable)
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

config_path = os.path.join(base_dir, 'appsettings.json')

try:
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
except FileNotFoundError:
    log.error(f"Критическая ошибка: файл {config_path} не найден!")
    sys.exit(1)
except json.JSONDecodeError as e:
    log.error(f"Ошибка чтения JSON в appsettings.json: {e}")
    sys.exit(1)

# ===================== НАСТРОЙКИ =====================
# PostgreSQL
PG_HOST = config["Postgres"]["Host"]
PG_PORT = config["Postgres"]["Port"]
PG_DB = config["Postgres"]["Database"]
PG_USER = config["Postgres"]["User"]
PG_PASSWORD = config["Postgres"]["Password"]

# RabbitMQ
RABBIT_HOST = config["RabbitMQ"]["Host"]
RABBIT_USER = config["RabbitMQ"]["User"]
RABBIT_PASSWORD = config["RabbitMQ"]["Password"]
RABBIT_QUEUE = config["RabbitMQ"]["Queue"]

# Performance
BATCH_SIZE = config["Performance"]["BatchSize"]
LOG_EVERY = config["Performance"]["LogEvery"]
# ===================== POSTGRES =====================
pg_conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
pg_conn.autocommit = False
pg_cur = pg_conn.cursor()

# ===================== RABBIT =====================
credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=60
    )
)
channel = connection.channel()

# ===================== STATE =====================
batch = []
delivery_tags = []
total_processed = 0

# ===================== XML PARSER =====================
def parse_message(body: bytes):
    # защита от пустых сообщений
    if not body or not body.strip():
        raise ValueError("empty body")

    # безопасный XML
    try:
        root = ET.fromstring(body)
    except Exception as e:
        raise ValueError(f"bad xml: {e}")

    record = root.find("RECORD")
    if record is None:
        raise ValueError("no RECORD")

    car_id = record.get("NCAR")
    if not car_id:
        raise ValueError("no NCAR")

    # DATETIME
    try:
        dt = datetime.strptime(
            record.get("DATETIME"),
            "%m/%d/%Y %I:%M:%S %p"
        )
    except Exception:
        dt = None

    def f(v):
        return float(v) if v not in (None, "") else None

    def i(v):
        return int(v) if v not in (None, "") else None

    def b(v):
        return v == "True"

    return (
        car_id,                         # car_id
        dt,
        f(record.get("LATITUDE")),
        f(record.get("LONGITUDE")),
        f(record.get("ANGLE")),
        f(record.get("SPEED")),
        b(record.get("ENGINE")),
        i(record.get("SATELLITES")),
        i(record.get("GSM")),
        b(record.get("LIGHT")),
        b(record.get("BUZZER")),
        record.get("STATE")             # просто сохраняем, не используем
    )

# ===================== BATCH PROCESS =====================
def process_batch():
    global batch, delivery_tags, total_processed

    if not batch:
        return

    try:
        navs = [
            (
                b[0], b[1], b[2], b[3], b[4], b[5],
                b[6], b[7], b[8], b[9], b[10]
            )
            for b in batch
        ]

        pg_cur.executemany(
            """
            INSERT INTO g_nav
            (car_id, dt, latitude, longitude, angle, speed,
             engine, satellites, gsm, light, buzzer)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            navs
        )

        pg_conn.commit()

        # ACK только после commit
        for tag in delivery_tags:
            channel.basic_ack(tag)

        total_processed += len(batch)

        if total_processed % LOG_EVERY == 0:
            log.info(f"processed={total_processed}")

    except Exception as e:
        pg_conn.rollback()
        log.error(f"batch rollback: {e}")

    finally:
        batch.clear()
        delivery_tags.clear()

# ===================== CALLBACK =====================
def callback(ch, method, properties, body):
    global batch, delivery_tags

    try:
        msg = parse_message(body)
        batch.append(msg)
        delivery_tags.append(method.delivery_tag)

        if len(batch) >= BATCH_SIZE:
            process_batch()

    except ValueError as e:
        # ❗ любые битые / пустые / кривые сообщения
        log.warning(f"skip message: {e}")
        ch.basic_ack(method.delivery_tag)

    except Exception as e:
        log.error(f"unexpected error: {e}")
        ch.basic_ack(method.delivery_tag)

# ===================== SHUTDOWN =====================
def shutdown(sig, frame):
    log.info("Shutdown received, flushing batch")
    process_batch()
    try:
        pg_cur.close()
        pg_conn.close()
        connection.close()
    finally:
        sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ===================== START =====================
channel.basic_qos(prefetch_count=BATCH_SIZE)
channel.basic_consume(
    queue=RABBIT_QUEUE,
    on_message_callback=callback,
    auto_ack=False
)

log.info(f"Consumer started batch={BATCH_SIZE}")
channel.start_consuming()
