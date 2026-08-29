-- THE CORE METRIC OF ATMOSYNC: "Spoilage Arbitrage"
--
-- For every container, estimate whether it will spoil before it reaches its
-- primary destination market, and if so, whether rerouting to a nearby
-- secondary market captures more value than pushing on and selling degraded
-- (or unsellable) stock at the primary market.
--
--   estimated_hours_to_spoilage   <- derived from the Spoilage Risk Index
--   remaining_hours_to_primary    <- assumes a fixed 96-hour planned voyage
--   degradation_penalty           <- fraction of remaining transit spent
--                                     already spoiled, if the container
--                                     continues on to the primary market
--   spoilage_arbitrage_usd_per_ton = secondary_price - degraded_primary_price

with health as (
    select * from {{ ref('container_health_snapshot') }}
),

prices as (
    select * from {{ ref('stg_commodity_prices') }}
),

primary_prices as (
    select cargo_type, market as primary_market, price_usd_per_ton as primary_price
    from prices
    where market_tier = 'Primary'
),

secondary_prices as (
    -- a cargo type can have multiple secondary-market options; pick the
    -- nearest one (fastest to reach = best chance of beating spoilage)
    select cargo_type, market as secondary_market, price_usd_per_ton as secondary_price,
           distance_from_route_km
    from (
        select
            *,
            row_number() over (partition by cargo_type order by distance_from_route_km asc) as rn
        from prices
        where market_tier = 'Secondary'
    )
    where rn = 1
),

voyage_assumptions as (
    select
        h.*,
        130 as planned_voyage_hours,         -- fixed assumption: telemetry snapshot is taken mid-voyage,
                                              -- with real transit still ahead before reaching the primary market
        20 as reroute_speed_kmh,             -- avg diversion speed to a secondary port
        greatest(2, 48 - h.spoilage_risk_index * 0.4) as estimated_hours_to_spoilage
    from health h
),

joined as (
    select
        v.*,
        pp.primary_market,
        pp.primary_price,
        sp.secondary_market,
        sp.secondary_price,
        sp.distance_from_route_km,
        (v.planned_voyage_hours - v.transit_hour) as remaining_hours_to_primary,
        sp.distance_from_route_km / v.reroute_speed_kmh as hours_to_reach_secondary
    from voyage_assumptions v
    left join primary_prices pp   on v.cargo_type = pp.cargo_type
    left join secondary_prices sp on v.cargo_type = sp.cargo_type
),

scored as (
    select
        *,
        case
            when estimated_hours_to_spoilage < remaining_hours_to_primary then true
            else false
        end as at_risk_flag,

        -- fraction of the remaining primary-market transit that would occur AFTER spoilage
        round(least(1.0, greatest(0.0,
            (remaining_hours_to_primary - estimated_hours_to_spoilage) / nullif(remaining_hours_to_primary, 0)
        )), 3) as degradation_penalty,

        case
            when hours_to_reach_secondary <= estimated_hours_to_spoilage then true
            else false
        end as secondary_reachable_in_time
    from joined
),

final as (
    select
        container_id,
        cargo_type,
        origin_port,
        transit_hour,
        spoilage_risk_index,
        round(estimated_hours_to_spoilage, 1) as estimated_hours_to_spoilage,
        remaining_hours_to_primary,
        at_risk_flag,
        primary_market,
        primary_price,
        round(primary_price * (1 - degradation_penalty), 2) as degraded_primary_value_usd_per_ton,
        secondary_market,
        secondary_price,
        round(hours_to_reach_secondary, 1) as hours_to_reach_secondary,
        secondary_reachable_in_time,
        round(secondary_price - (primary_price * (1 - degradation_penalty)), 2) as spoilage_arbitrage_usd_per_ton,
        case
            when at_risk_flag and secondary_reachable_in_time
                 and (secondary_price - (primary_price * (1 - degradation_penalty))) > 0
                then 'REROUTE to ' || secondary_market
            when at_risk_flag then 'AT RISK - no viable reroute in time'
            else 'ON TRACK - continue to ' || primary_market
        end as recommendation
    from scored
)

select * from final
order by spoilage_arbitrage_usd_per_ton desc
