"""Mock historical/secondary market commodity pricing across all cargo types.
Represents the historical pricing table that would already live in Snowflake."""
import csv

# cargo, market, price_usd_per_ton, distance_from_route_km, market_tier
PRICES = [
    # Avocados -> Rotterdam
    ("Avocados", "Rotterdam", 2400, 0, "Primary"),
    ("Avocados", "Antwerp", 2280, 90, "Secondary"),
    ("Avocados", "Le Havre", 2350, 620, "Secondary"),

    # Bananas -> Hamburg
    ("Bananas", "Hamburg", 950, 0, "Primary"),
    ("Bananas", "Bremerhaven", 910, 120, "Secondary"),
    ("Bananas", "Antwerp", 930, 380, "Secondary"),

    # Strawberries -> Paris
    ("Strawberries", "Paris", 3100, 0, "Primary"),
    ("Strawberries", "Lyon", 2950, 460, "Secondary"),
    ("Strawberries", "Marseille", 3000, 780, "Secondary"),

    # Mangoes -> Dubai
    ("Mangoes", "Dubai", 1800, 0, "Primary"),
    ("Mangoes", "Abu Dhabi", 1750, 140, "Secondary"),
    ("Mangoes", "Muscat", 1700, 460, "Secondary"),

    # Grapes -> New York
    ("Grapes", "New York", 2600, 0, "Primary"),
    ("Grapes", "Newark", 2540, 25, "Secondary"),
    ("Grapes", "Philadelphia", 2500, 150, "Secondary"),

    # Salmon -> Boston
    ("Salmon", "Boston", 4200, 0, "Primary"),
    ("Salmon", "Portland", 4050, 170, "Secondary"),
    ("Salmon", "New York", 4150, 300, "Secondary"),

    # Blueberries -> Miami
    ("Blueberries", "Miami", 3600, 0, "Primary"),
    ("Blueberries", "Tampa", 3450, 330, "Secondary"),
    ("Blueberries", "Jacksonville", 3500, 550, "Secondary"),

    # Asparagus -> Marseille
    ("Asparagus", "Marseille", 2900, 0, "Primary"),
    ("Asparagus", "Barcelona", 2750, 510, "Secondary"),
    ("Asparagus", "Genoa", 2820, 400, "Secondary"),
]

with open("/home/claude/atmosync/data/commodity_prices.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["cargo_type", "market", "price_usd_per_ton", "distance_from_route_km", "market_tier"])
    writer.writerows(PRICES)

print(f"Generated commodity_prices.csv with {len(PRICES)} rows across 8 cargo types")
