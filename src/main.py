import os
import requests
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()  # Loads .env file into environment variables

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)


def normalize_timestamp(ts: str) -> str:
    # Remove microseconds and convert +00:00 suffix to Z
    if '.' in ts:
        ts = ts.split('.')[0] + 'Z'
    elif ts.endswith('+00:00'):
        ts = ts[:-6] + 'Z'
    return ts


def fetch_transformers():
    response = supabase.table("transformers").select("id,location").execute()
    if response.data is None:
        print("No data returned or error occurred.")
        return None
    return response.data


def fetch_existing_timestamps(transformer_id, timestamps):
    iso_times = [
        ts.tz_localize('UTC').isoformat().replace('+00:00', 'Z') if ts.tzinfo is None else ts.isoformat().replace('+00:00', 'Z')
        for ts in timestamps
    ]
    existing = set()
    batch_size = 50

    for i in range(0, len(iso_times), batch_size):
        batch = iso_times[i:i + batch_size]
        response = supabase.table("temperature_readings") \
            .select("timestamp") \
            .eq("transformer_id", transformer_id) \
            .in_("timestamp", batch) \
            .execute()
        if response.data:
            for record in response.data:
                ts = normalize_timestamp(record['timestamp'])
                existing.add(ts)
    return existing


def fetch_missing_temperature_readings(transformer_id, timestamps):
    iso_times = [
        ts.tz_localize('UTC').isoformat().replace('+00:00', 'Z') if ts.tzinfo is None else ts.isoformat().replace('+00:00', 'Z')
        for ts in timestamps
    ]
    missing = set()
    batch_size = 50

    for i in range(0, len(iso_times), batch_size):
        batch = iso_times[i:i + batch_size]
        response = supabase.table("temperature_readings") \
            .select("timestamp") \
            .eq("transformer_id", transformer_id) \
            .in_("timestamp", batch) \
            .is_("ambient_temperature", None) \
            .execute()
        if response.data:
            for record in response.data:
                ts = normalize_timestamp(record['timestamp'])
                missing.add(ts)
    return missing


def fetch_weather(lat, lng):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&hourly=temperature_2m&past_days=31"
    r = requests.get(url)
    if r.status_code != 200:
        print(f"Failed to fetch weather data: {r.status_code}")
        return None
    data = r.json().get('hourly')
    print("Keys in hourly data:", data.keys())  # Debug line
    if not data or 'time' not in data or 'temperature_2m' not in data:
        print("Incomplete weather data received")
        return None
    df = pd.DataFrame({
        'time': data['time'],
        'ambient_temperature': data['temperature_2m']
    })
    df['time'] = pd.to_datetime(df['time'])
    return df


def filter_to_update(df, existing_timestamps, missing_timestamps):
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')
    df['iso_time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    filtered_df = df[df['iso_time'].isin(existing_timestamps & missing_timestamps)].copy()
    filtered_df.drop(columns=['iso_time'], inplace=True)
    return filtered_df


def update_ambient_temperature(row):
    update_data = {
        "ambient_temperature": row["ambient_temperature"]
    }
    ts_str = row["time"].strftime('%Y-%m-%dT%H:%M:%SZ')
    response = supabase.table("temperature_readings") \
        .update(update_data) \
        .eq("transformer_id", row["transformer_id"]) \
        .eq("timestamp", ts_str) \
        .execute()
    
    # Check if response.data contains updated record
    if response.data and len(response.data) > 0:
        print(f"Successfully updated transformer {row['transformer_id']} at {ts_str}")
        return True
    else:
        print(f"Failed to update transformer {row['transformer_id']} at {ts_str} (no matching row)")
        return False




def main():
    all_dfs = []
    transformers = fetch_transformers()
    if not transformers:
        print("No transformers found.")
        return

    for i in transformers:
        transformer_id = i["id"]

        location = i.get("location", {})
        if not location:
            continue

        lat = location.get('lat')
        lng = location.get('lng')
        if lat is None or lng is None:
            continue

        df = fetch_weather(lat, lng)
        if df is None or df.empty:
            continue

        existing_ts = fetch_existing_timestamps(transformer_id, df['time'].tolist())
        missing_ts = fetch_missing_temperature_readings(transformer_id, df['time'].tolist())

        print(f"Transformer {transformer_id}: {len(existing_ts)} existing timestamps")
        print(f"Transformer {transformer_id}: {len(missing_ts)} missing ambient temperature timestamps")
        print(f"Transformer {transformer_id}: Intersection size: {len(existing_ts & missing_ts)}")

        filtered_df = filter_to_update(df, existing_ts, missing_ts)

        print(f"Transformer {transformer_id}: filtered to {len(filtered_df)} new temperature updates")

        if filtered_df.empty:
            continue

        filtered_df['transformer_id'] = transformer_id
        all_dfs.append(filtered_df)

    if not all_dfs:
        print("No new ambient temperature data to update.")
        return

    final_df = pd.concat(all_dfs, ignore_index=True)

    update_count = 0
    for _, row in final_df.iterrows():
        try:
            updated = update_ambient_temperature(row)
            if updated:
                update_count += 1
        except Exception as e:
            print(f"Exception updating transformer {row['transformer_id']} at {row['time']}: {e}")
    print(f"Updated ambient temperature for {update_count} new data points in Supabase.")


if __name__ == "__main__":
    main()
