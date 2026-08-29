-- Cleans raw Kafka-sourced telemetry and attaches the ideal storage band
-- for each cargo type so downstream models can compute deviation/degradation.

with source as (
    select * from raw.container_telemetry
),

ideal_bands as (
    -- reference bands per cargo type (would normally be its own seed/dim table)
    select * from (
        values
            ('Avocados',      5.5, 90),
            ('Bananas',      13.5, 90),
            ('Strawberries',  1.0, 92),
            ('Mangoes',      10.0, 88),
            ('Grapes',        0.5, 91),
            ('Salmon',       -1.0, 95),
            ('Blueberries',   0.0, 93),
            ('Asparagus',     2.5, 95)
    ) as t(cargo_type, ideal_temp_c, ideal_humidity_pct)
)

select
    s.event_time,
    s.container_id,
    s.cargo_type,
    s.origin_port,
    s.destination_market,
    s.transit_hour,
    s.temperature_c,
    s.humidity_pct,
    s.vibration_g,
    b.ideal_temp_c,
    b.ideal_humidity_pct,
    round(s.temperature_c - b.ideal_temp_c, 2)      as temp_deviation_c,
    round(b.ideal_humidity_pct - s.humidity_pct, 2) as humidity_deficit_pct
from source s
left join ideal_bands b on s.cargo_type = b.cargo_type
