"""
AtmoSync - Step 1: IoT Telemetry Simulator (SCALED)
------------------------------------------------------
Generates high-frequency sensor readings (temperature, humidity, vibration)
for a large fleet of refrigerated shipping containers across many cargo
types and trade routes - tens of thousands of events, comparable in shape
to a real day or two of a mid-size reefer fleet's telemetry stream.

In production this feeds a Kafka producer event-by-event
(see 02_kafka_producer_consumer.py). Scale knobs are at the top of the file.
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(7)

OUTPUT_PATH = "/home/claude/atmosync/data/raw_telemetry.csv"

# ----------------------------- SCALE KNOBS -----------------------------
CONTAINERS_PER_CARGO = 8        # fleet size per cargo type
HOURS_OF_TRANSIT = 96             # 4-day voyages
READINGS_PER_HOUR = 12            # every 5 minutes -> high-throughput stream
DRIFT_RATE = 0.18                 # fraction of containers that develop a mid-voyage anomaly
# with 8 cargo types x 8 containers x 96h x 12/hr => ~73,700 telemetry events
# -------------------------------------------------------------------------

CARGO_PROFILES = [
    {"cargo": "Avocados",     "ideal_temp": 5.5,  "ideal_humidity": 90, "origin": "Mombasa",    "primary_market": "Rotterdam"},
    {"cargo": "Bananas",      "ideal_temp": 13.5, "ideal_humidity": 90, "origin": "Guayaquil",  "primary_market": "Hamburg"},
    {"cargo": "Strawberries", "ideal_temp": 1.0,  "ideal_humidity": 92, "origin": "Huelva",     "primary_market": "Paris"},
    {"cargo": "Mangoes",      "ideal_temp": 10.0, "ideal_humidity": 88, "origin": "Karachi",    "primary_market": "Dubai"},
    {"cargo": "Grapes",       "ideal_temp": 0.5,  "ideal_humidity": 91, "origin": "Valparaiso", "primary_market": "New York"},
    {"cargo": "Salmon",       "ideal_temp": -1.0, "ideal_humidity": 95, "origin": "Bergen",     "primary_market": "Boston"},
    {"cargo": "Blueberries",  "ideal_temp": 0.0,  "ideal_humidity": 93, "origin": "Lima",       "primary_market": "Miami"},
    {"cargo": "Asparagus",    "ideal_temp": 2.5,  "ideal_humidity": 95, "origin": "Tangier",    "primary_market": "Marseille"},
]


def build_fleet():
    fleet = []
    for profile in CARGO_PROFILES:
        prefix = profile["cargo"][:3].upper()
        for n in range(1, CONTAINERS_PER_CARGO + 1):
            fleet.append({
                "container_id": f"{prefix}-{n:03d}",
                **profile,
            })
    return fleet


def assign_drift_events(fleet):
    """Randomly select a subset of containers to develop a mid-voyage
    micro-climate anomaly, with varied onset time and severity."""
    drift_map = {}
    n_drift = max(1, int(len(fleet) * DRIFT_RATE))
    drifting = random.sample(fleet, n_drift)
    for c in drifting:
        drift_map[c["container_id"]] = {
            "start_hour": random.randint(24, HOURS_OF_TRANSIT - 20),
            "magnitude": round(random.uniform(3.5, 8.5), 1),
            "humidity_drop": round(random.uniform(5, 14), 1),
        }
    return drift_map


def generate_readings():
    fleet = build_fleet()
    drift_map = assign_drift_events(fleet)
    start_time = datetime(2026, 8, 1, 0, 0, 0)
    rows = []

    for container in fleet:
        cid = container["container_id"]
        drift = drift_map.get(cid)

        for hour in range(HOURS_OF_TRANSIT):
            for tick in range(READINGS_PER_HOUR):
                minute = tick * (60 // READINGS_PER_HOUR)
                ts = start_time + timedelta(hours=hour, minutes=minute)

                temp = container["ideal_temp"] + random.gauss(0, 0.3)
                humidity = container["ideal_humidity"] + random.gauss(0, 1.5)
                vibration = abs(random.gauss(0.4, 0.15))

                if drift and hour >= drift["start_hour"]:
                    progress = min(1.0, (hour - drift["start_hour"]) / 20)
                    temp += drift["magnitude"] * progress
                    humidity -= drift["humidity_drop"] * progress
                    vibration += 0.3 * progress * random.random()

                rows.append((
                    ts.isoformat(), cid, container["cargo"], container["origin"],
                    container["primary_market"], round(temp, 2), round(max(0, humidity), 2),
                    round(vibration, 3), hour,
                ))
    return rows, drift_map


if __name__ == "__main__":
    rows, drift_map = generate_readings()
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_time", "container_id", "cargo_type", "origin_port",
                          "destination_market", "temperature_c", "humidity_pct", "vibration_g", "transit_hour"])
        writer.writerows(rows)

    n_containers = CONTAINERS_PER_CARGO * len(CARGO_PROFILES)
    print(f"Generated {len(rows):,} telemetry events across {n_containers} containers "
          f"({len(CARGO_PROFILES)} cargo types x {CONTAINERS_PER_CARGO} containers each)")
    print(f"Drift events injected on {len(drift_map)} containers: {sorted(drift_map.keys())}")
