"""
AtmoSync - Step 3: Cloud Data Warehouse Load (Snowflake)
------------------------------------------------------------
Lands the streamed Kafka events + reference data into RAW tables.

In production this connects to Snowflake via `snowflake.connector` (or
Snowpipe for continuous micro-batch loads straight off the Kafka topic).
This sandbox has no network route to a Snowflake account, so we use DuckDB
as a local, file-based warehouse - it speaks the same SQL dialect dbt
targets, so every model in dbt_project/ is unmodified when you point the
`profiles.yml` target at Snowflake instead (see profiles.yml comments).
"""
import duckdb

DB_PATH = "/home/claude/atmosync/atmosync.duckdb"
STREAMED = "/home/claude/atmosync/data/streamed_telemetry.jsonl"
PRICES_CSV = "/home/claude/atmosync/data/commodity_prices.csv"

con = duckdb.connect(DB_PATH)
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

# --- RAW.CONTAINER_TELEMETRY (from Kafka topic) ---
# At tens-of-thousands-of-events scale, DuckDB's native JSON reader (vectorized,
# columnar ingest) is used instead of row-by-row executemany - this is the same
# pattern a real Snowpipe / COPY INTO continuous-load job uses against a
# landed JSON stream.
con.execute("DROP TABLE IF EXISTS raw.container_telemetry;")
con.execute(f"""
    CREATE TABLE raw.container_telemetry AS
    SELECT
        event_time::TIMESTAMP        AS event_time,
        container_id,
        cargo_type,
        origin_port,
        destination_market,
        temperature_c::DOUBLE        AS temperature_c,
        humidity_pct::DOUBLE         AS humidity_pct,
        vibration_g::DOUBLE          AS vibration_g,
        transit_hour::INTEGER        AS transit_hour
    FROM read_json_auto('{STREAMED}');
""")

# --- RAW.COMMODITY_PRICES (reference data) ---
con.execute("DROP TABLE IF EXISTS raw.commodity_prices;")
con.execute(f"CREATE TABLE raw.commodity_prices AS SELECT * FROM read_csv_auto('{PRICES_CSV}');")

counts = con.execute("""
    SELECT (SELECT COUNT(*) FROM raw.container_telemetry) AS telemetry_rows,
           (SELECT COUNT(*) FROM raw.commodity_prices) AS price_rows
""").fetchone()
print(f"Loaded raw.container_telemetry: {counts[0]} rows | raw.commodity_prices: {counts[1]} rows")
print(f"Warehouse file: {DB_PATH}")
con.close()
