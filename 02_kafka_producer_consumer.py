"""
AtmoSync - Step 2: Streaming Ingestion Layer (Apache Kafka)
-------------------------------------------------------------
This is the REAL producer/consumer code you would run against a live Kafka
cluster (e.g. Confluent Cloud, MSK, or a self-hosted broker). It is written
against `kafka-python` and topic `container.telemetry.raw`.

    pip install kafka-python
    docker run -p 9092:9092 apache/kafka:latest   # local broker for dev

Because this sandbox environment has no network access to a Kafka broker,
the `if __name__` block at the bottom falls back to a local JSON-lines queue
(LocalBrokerShim) so you can still execute the full pipeline end-to-end here.
Swap `LocalBrokerShim` for `KafkaProducer`/`KafkaConsumer` unchanged when you
point this at a real cluster - the message contract does not change.
"""
import csv
import json
import time

TOPIC = "container.telemetry.raw"
SOURCE_CSV = "/home/claude/atmosync/data/raw_telemetry.csv"
STREAMED_OUTPUT = "/home/claude/atmosync/data/streamed_telemetry.jsonl"

# ---------------------------------------------------------------------------
# REAL Kafka code (reference implementation - requires a running broker)
# ---------------------------------------------------------------------------
REAL_PRODUCER_CODE = '''
from kafka import KafkaProducer
import json, csv

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,          # micro-batch for throughput
    compression_type="snappy",
)

with open("data/raw_telemetry.csv") as f:
    for row in csv.DictReader(f):
        producer.send("container.telemetry.raw", value=row)
producer.flush()
'''

REAL_CONSUMER_CODE = '''
from kafka import KafkaConsumer
import json, snowflake.connector

consumer = KafkaConsumer(
    "container.telemetry.raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="snowflake-sink",
)

conn = snowflake.connector.connect(...)  # RAW.CONTAINER_TELEMETRY table
for message in consumer:
    event = message.value
    conn.cursor().execute(
        "INSERT INTO RAW.CONTAINER_TELEMETRY VALUES (%(event_time)s, %(container_id)s, ...)",
        event,
    )
'''


# ---------------------------------------------------------------------------
# Local shim so this demo runs without a broker - same message contract
# ---------------------------------------------------------------------------
class LocalBrokerShim:
    """Drop-in stand-in for KafkaProducer/KafkaConsumer in this sandbox.
    Streams straight to disk in micro-batches so tens of thousands of
    events don't need to sit in memory at once (mirrors a producer's
    internal batching behavior)."""

    def __init__(self, topic_file, batch_size=2000):
        self.f = open(topic_file, "w")
        self.batch_size = batch_size
        self._batch = []

    def send(self, value: dict):
        self._batch.append(value)
        if len(self._batch) >= self.batch_size:
            self._write_batch()

    def _write_batch(self):
        for event in self._batch:
            self.f.write(json.dumps(event) + "\n")
        self._batch.clear()

    def flush(self):
        self._write_batch()
        self.f.close()


def produce():
    producer = LocalBrokerShim(STREAMED_OUTPUT)
    with open(SOURCE_CSV) as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            producer.send(row)
            count += 1
            if count % 20000 == 0:
                print(f"[producer] ...{count:,} events published so far")
    producer.flush()
    print(f"[producer] published {count:,} events to topic '{TOPIC}' -> {STREAMED_OUTPUT}")


def consume_preview(n=3):
    with open(STREAMED_OUTPUT) as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            print(f"[consumer] {json.loads(line)}")


if __name__ == "__main__":
    produce()
    consume_preview()
