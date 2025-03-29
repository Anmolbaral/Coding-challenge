import os
import pandas as pd
import json

# Define base directory relative to this script's location (three levels up)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# Construct file paths
INSTRUMENT_FILE = os.path.join(BASE_DIR, "data/raw/instrument_data.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "predictions/predictions_task_3.json")

# Anomaly detection configuration for each station
# (The "pollutant" key is included per the challenge spec but is not used in this simple rule-based detection)
ANOMALY_CONFIG = {
    "205": {"pollutant": "SO2",   "start": "2023-11-01 00:00:00", "end": "2023-11-30 23:00:00"},
    "209": {"pollutant": "NO2",   "start": "2023-09-01 00:00:00", "end": "2023-09-30 23:00:00"},
    "223": {"pollutant": "O3",    "start": "2023-07-01 00:00:00", "end": "2023-07-31 23:00:00"},
    "224": {"pollutant": "CO",    "start": "2023-10-01 00:00:00", "end": "2023-10-31 23:00:00"},
    "226": {"pollutant": "PM10",  "start": "2023-08-01 00:00:00", "end": "2023-08-31 23:00:00"},
    "227": {"pollutant": "PM2.5", "start": "2023-12-01 00:00:00", "end": "2023-12-31 23:00:00"}
}

def detect_anomalies_for_station(df, station_code, start, end):
    """
    For a given station and period, count the number of anomaly records (Instrument status != 0)
    for each hour in the period.
    """
    # Filter for the specific station
    df_station = df[df["Station code"] == int(station_code)].copy()
    if df_station.empty:
        return {}

    # Filter by the time period
    mask = (df_station["Measurement date"] >= start) & (df_station["Measurement date"] <= end)
    df_station = df_station[mask]
    
    # Generate an hourly time range for the period
    hourly_range = pd.date_range(start=start, end=end, freq="H")
    
    # Initialize dictionary to store anomaly count per hour
    anomaly_counts = {}
    
    # Group the data by hour (floor the timestamp to the hour)
    df_station["hour_ts"] = df_station["Measurement date"].dt.floor("H")
    
    # An anomaly is defined as any record where Instrument status != 0
    df_anomalies = df_station[df_station["Instrument status"] != 0]
    
    # Count anomalies per hour using groupby
    anomaly_group = df_anomalies.groupby("hour_ts").size().to_dict()
    
    # For each timestamp in our hourly range, assign the count (0 if missing)
    for ts in hourly_range:
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        count = anomaly_group.get(ts, 0)
        anomaly_counts[ts_str] = int(count)
    
    return anomaly_counts


def main():
    # Load instrument data with Measurement date parsed as datetime
    try:
        df_instrument = pd.read_csv(INSTRUMENT_FILE, parse_dates=["Measurement date"])
    except Exception as e:
        print("Error loading instrument data:", e)
        return

    result = {"target": {}}
    
    # Loop through each configuration entry and detect anomalies
    for station, config in ANOMALY_CONFIG.items():
        start = config["start"]
        end = config["end"]
        anomalies = detect_anomalies_for_station(df_instrument, station, start, end)
        result["target"][station] = anomalies
    
    # Write the predictions to the output JSON file
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Anomaly detection results for Task 3 have been written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
