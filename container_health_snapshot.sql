-- One row per container = its current (latest) health snapshot, using a
-- trailing 2-hour rolling average so single noisy readings don't trigger
-- false alarms. This is what the "container health" panel of the dashboard
-- reads from.

with telemetry as (
    select * from {{ ref('stg_telemetry') }}
),

latest_hour as (
    select container_id, max(transit_hour) as max_hour
    from telemetry
    group by container_id
),

trailing_window as (
    select t.*
    from telemetry t
    join latest_hour lh
      on t.container_id = lh.container_id
     and t.transit_hour >= lh.max_hour - 1   -- trailing ~2 hours
),

rolled_up as (
    select
        container_id,
        any_value(cargo_type)           as cargo_type,
        any_value(origin_port)          as origin_port,
        any_value(destination_market)   as destination_market,
        max(transit_hour)               as transit_hour,
        avg(temp_deviation_c)           as avg_temp_deviation_c,
        avg(humidity_deficit_pct)       as avg_humidity_deficit_pct,
        avg(vibration_g)                as avg_vibration_g
    from trailing_window
    group by container_id
)

select
    container_id,
    cargo_type,
    origin_port,
    destination_market,
    transit_hour,
    round(avg_temp_deviation_c, 2)     as avg_temp_deviation_c,
    round(avg_humidity_deficit_pct, 2) as avg_humidity_deficit_pct,
    round(avg_vibration_g, 3)          as avg_vibration_g,
    -- Spoilage Risk Index (0-100): weighted composite of the three sensor signals.
    -- Only *positive* temp deviation and humidity deficit hurt produce; vibration always does.
    round(least(100, greatest(0,
        10 * greatest(avg_temp_deviation_c, 0)
      + 2  * greatest(avg_humidity_deficit_pct, 0)
      + 15 * avg_vibration_g
    )), 1) as spoilage_risk_index
from rolled_up
