import os
import requests
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv() # Loads .env file into environment variables

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def fetch_transformers():
    return supabase.table("transformers").select("id,location").execute().data

def fetch_weather(lat, lng):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&hourly=temperature_2m&past_days=31"
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()['hourly']

def update_ambient_temperature(row):
    update_data = {
        "ambient_temperature": row["ambient_temperature"]
    }
    response = supabase.table("temperature_readings") \
        .update(update_data) \
        .eq("transformer_id", row["transformer_id"]) \
        .eq("timestamp", row["time"].isoformat()) \
        .execute()
    return response

def main():
    all_dfs = []
    transformers = fetch_transformers()
    if not transformers:
        print("No transformers found.")
        return
    for i in transformers:
        location = i.get("location", {})
        if not location:
            continue 

        lat = location.get('lat')
        lng = location.get('lng')
        if lat is None or lng is None:
            continue

        weather = fetch_weather(lat, lng)
        if not weather:
            continue

        df = pd.DataFrame({
            'time': weather['time'],
            'ambient_temperature': weather['temperature_2m']
        })
        df['time'] = pd.to_datetime(df['time'])
        df['transformer_id'] = i["id"]
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)

    update_count = 0
    for _, row in final_df.iterrows():
        resp = update_ambient_temperature(row)
        if resp.data is not None:
            print(f"Updated transformer {row['transformer_id']} at {row['time']}")
            update_count += 1
        else:
            print(f"Failed to update transformer {row['transformer_id']} at {row['time']}: {resp.error}")
    print(f"Updated ambient temperature for {update_count} data points in Supabase.")

if __name__ == "__main__":
    main()