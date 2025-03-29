import os
import pandas as pd
import json

# Define base directory relative to this script's location
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# Construct file paths
MEASUREMENT_FILE = os.path.join(BASE_DIR, "data/raw/measurement_data.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "predictions/predictions_task_2.json")

# Forecast configuration for each station
FORECAST_CONFIG = {
    "206": {"pollutant": "SO2",   "start": "2023-07-01 00:00:00", "end": "2023-07-31 23:00:00"},
    "211": {"pollutant": "NO2",   "start": "2023-08-01 00:00:00", "end": "2023-08-31 23:00:00"},
    "217": {"pollutant": "O3",    "start": "2023-09-01 00:00:00", "end": "2023-09-30 23:00:00"},
    "219": {"pollutant": "CO",    "start": "2023-10-01 00:00:00", "end": "2023-10-31 23:00:00"},
    "225": {"pollutant": "PM10",  "start": "2023-11-01 00:00:00", "end": "2023-11-30 23:00:00"},
    "228": {"pollutant": "PM2.5", "start": "2023-12-01 00:00:00", "end": "2023-12-31 23:00:00"}
}

def forecast_station(df, station_code, pollutant, forecast_period):
    # Filter historical data for the given station
    df_station = df[df['Station code'] == int(station_code)]
    if df_station.empty or pollutant not in df_station.columns:
        # If no data is available, return a forecast of zeros
        return {ts: 0.0 for ts in forecast_period}
    
    # Create a column for hour-of-day
    df_station['hour'] = df_station['Measurement date'].dt.hour
    # Compute the average value for the pollutant for each hour (0-23)
    hourly_avg = df_station.groupby('hour')[pollutant].mean().to_dict()
    
    # Use the overall mean as a fallback for hours missing in the data
    overall_mean = df_station[pollutant].mean()
    
    # Generate the forecast by assigning the hourly average to each timestamp
    forecast = {}
    for ts in forecast_period:
        hour = pd.to_datetime(ts).hour
        value = hourly_avg.get(hour, overall_mean)
        forecast[ts] = round(value, 5)
    return forecast


def main():
    # Load measurement data
    try:
        df_measure = pd.read_csv(MEASUREMENT_FILE, parse_dates=["Measurement date"])
    except Exception as e:
        print("Error loading measurement data:", e)
        return
    
    result = {"target": {}}
    
    # Loop through each forecast configuration
    for station, config in FORECAST_CONFIG.items():
        pollutant = config["pollutant"]
        start = config["start"]
        end = config["end"]
        # Generate forecast timestamps with hourly frequency
        forecast_period = pd.date_range(start=start, end=end, freq="H").strftime("%Y-%m-%d %H:%M:%S").tolist()
        # Get the forecast for this station and pollutant
        station_forecast = forecast_station(df_measure, station, pollutant, forecast_period)
        result["target"][station] = station_forecast
    
    # Write predictions to predictions_task_2.json
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print("Forecast predictions for Task 2 have been written to predictions/predictions_task_2.json")


if __name__ == "__main__":
    main()