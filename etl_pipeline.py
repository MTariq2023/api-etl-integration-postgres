import os
import time
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from zeep import Client
from zeep.transports import Transport
from requests import Session

#Setup 
load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

if not OPENWEATHER_API_KEY:
    raise RuntimeError("Missing OPENWEATHER_API_KEY in .env")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL in .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Ensure schema is present
schema_sql = Path("schema.sql").read_text(encoding="utf-8")
with engine.begin() as conn:
    conn.execute(text(schema_sql))

# Default demo list 
CITIES = [
    ("London", "GB"),
    ("New York", "US"),
    ("Toronto", "CA"),
    ("Paris", "FR"),
    ("Tokyo", "JP"),
]

# SOAP country info service (XML)
WSDL = "http://webservices.oorsprong.org/websamples.countryinfo/CountryInfoService.wso?WSDL"
session = Session()
session.verify = True
transport = Transport(session=session, timeout=30)
client = Client(wsdl=WSDL, transport=transport)


# Helpers
def fetch_weather_json(city: str, country_code: str) -> dict:
    """Fetch current weather (JSON) for a city, returns a flat dict."""
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q={city},{country_code}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return {
        "city": data.get("name"),
        "country": data.get("sys", {}).get("country"),
        "temperature_c": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
    }


def fetch_country_info(iso2: str) -> dict:
    """Fetch country details (SOAP/XML) for ISO2 code."""
    info = client.service.FullCountryInfo(iso2)
    pop = None
    try:
        pop = int(info.sPopulation)
    except Exception:
        pass
    return {
        "country": info.sName,
        "capital": info.sCapitalCity,
        "population": pop,
        "currency": info.sCurrencyISOCode,
    }


def safe_append(df: pd.DataFrame, table_name: str):
    """Append dataframe if not empty; drop exact duplicates before write."""
    if df is None or df.empty:
        return
    df = df.drop_duplicates()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)


#  Demo run over default cities 
weather_rows = []
for city, cc in CITIES:
    try:
        row = fetch_weather_json(city, cc)
        if row["city"] and row["country"]:
            weather_rows.append(row)
        time.sleep(0.35)
    except Exception as e:
        print(f"[weather] {city},{cc} failed: {e}")

weather_df = pd.DataFrame(weather_rows)
print("\n[Demo] Weather sample:")
print(weather_df.head())

# Collect unique ISO2 codes we actually fetched
iso2_codes = sorted({r["country"] for r in weather_rows if r.get("country")})
country_rows = []
for iso2 in iso2_codes:
    try:
        country_rows.append(fetch_country_info(iso2))
        time.sleep(0.25)
    except Exception as e:
        print(f"[country] {iso2} failed: {e}")

country_df = pd.DataFrame(country_rows)
print("\n[Demo] Country sample:")
print(country_df.head())

# Load demo results
safe_append(weather_df, "weather_data")
safe_append(country_df, "country_info")

#  Interactive mode (user adds any city/country)
try:
    choice = input("\nDo you want to add a custom city? (y/n): ").strip().lower()
except EOFError:
    choice = "n"

while choice == "y":
    city = input("Enter city name (e.g., Chicago): ").strip()
    iso2 = input("Enter 2-letter country code (e.g., US, GB, IN): ").strip().upper()

    if not city or not iso2:
        print("City and country code are required. Try again.")
    else:
        try:
            custom_weather = fetch_weather_json(city, iso2)
            print("Fetched weather:", custom_weather)
            safe_append(pd.DataFrame([custom_weather]), "weather_data")

            # Also fetch/update country info for that ISO code
            try:
                custom_country = fetch_country_info(iso2)
                print("Fetched country info:", custom_country)
                safe_append(pd.DataFrame([custom_country]), "country_info")
            except Exception as e:
                print(f"[country] {iso2} failed: {e}")

        except Exception as e:
            print(f"[weather] fetch failed for {city},{iso2}: {e}")

    try:
        choice = input("\nAdd another city? (y/n): ").strip().lower()
    except EOFError:
        break

print("\n✅ ETL complete.")

# Simple report (join-like view)
with engine.begin() as conn:
    # Show hottest cities from latest inserts (not strictly joined)
    hottest = conn.execute(
        text("""
            SELECT city, country, temperature_c, observed_at
            FROM weather_data
            ORDER BY observed_at DESC, temperature_c DESC
            LIMIT 10
        """)
    ).mappings().all()

    print("\nTop recent cities by temperature:")
    for r in hottest:
        print(f"  {r['city']} ({r['country']}): {r['temperature_c']} °C  @ {r['observed_at']}")

