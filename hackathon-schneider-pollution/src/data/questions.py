import os
import pandas as pd
import json

# Define base directory relative to this script's location
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Construct file paths
MEASUREMENT_FILE = os.path.join(BASE_DIR, "data/raw/measurement_data.csv")
INSTRUMENT_FILE = os.path.join(BASE_DIR, "data/raw/instrument_data.csv")
POLLUTANT_FILE = os.path.join(BASE_DIR, "data/raw/pollutant_data.csv")


def get_season(month):
    # Map month to season code: 1: Winter, 2: Spring, 3: Summer, 4: Autumn
    if month in [12, 1, 2]:
        return "1"  # Winter
    elif month in [3, 4, 5]:
        return "2"  # Spring
    elif month in [6, 7, 8]:
        return "3"  # Summer
    else:
        return "4"  # Autumn


def compute_task1():
    # Load measurement data
    try:
        df_measure = pd.read_csv(MEASUREMENT_FILE, parse_dates=["Measurement date"])
    except Exception as e:
        print("Error loading measurement data:", e)
        return None

    # Filter for normal measurements (assuming a column named 'status' holds the measurement status)
    if 'status' in df_measure.columns:
        df_normal = df_measure[df_measure['status'] == 0].copy()
    else:
        # If no status column, assume all measurements are normal
        df_normal = df_measure.copy()

    # Q1: Average daily SO2 concentration across all districts (overall average)
    q1 = round(df_normal['SO2'].mean(), 5)

    # Q2: Average CO per season at station 209
    df_209 = df_normal[df_normal['Station code'] == 209].copy()
    df_209['month'] = df_209['Measurement date'].dt.month
    df_209['season'] = df_209['month'].apply(get_season)
    q2_series = df_209.groupby('season')['CO'].mean().round(5)
    q2 = q2_series.to_dict()

    # Q3: Hour with highest variability (standard deviation) for O3 across all stations
    df_normal['hour'] = df_normal['Measurement date'].dt.hour
    std_by_hour = df_normal.groupby('hour')['O3'].std()
    q3 = int(std_by_hour.idxmax())

    # Load instrument data
    try:
        df_instrument = pd.read_csv(INSTRUMENT_FILE, parse_dates=["Measurement date"])
    except Exception as e:
        print("Error loading instrument data:", e)
        df_instrument = pd.DataFrame()

    # Q4: Station code with most 'Abnormal data' (code 9)
    if not df_instrument.empty and 'Instrument status' in df_instrument.columns:
        df_abnormal = df_instrument[df_instrument['Instrument status'] == 9]
        if not df_abnormal.empty:
            q4 = int(df_abnormal['Station code'].value_counts().idxmax())
        else:
            q4 = None
    else:
        q4 = None

    # Q5: Station code with most 'not normal' measurements (status != 0)
    if not df_instrument.empty and 'Instrument status' in df_instrument.columns:
        df_not_normal = df_instrument[df_instrument['Instrument status'] != 0]
        if not df_not_normal.empty:
            q5 = int(df_not_normal['Station code'].value_counts().idxmax())
        else:
            q5 = None
    else:
        q5 = None

    # Load pollutant data for Q6
    try:
        df_pollutant = pd.read_csv(POLLUTANT_FILE)
    except Exception as e:
        print("Error loading pollutant data:", e)
        df_pollutant = pd.DataFrame()

    # Q6: Count of Good, Normal, Bad, and Very bad records for PM2.5 pollutant
    # Assuming the pollutant data has an 'Item name' column to filter for PM2.5 and columns 'Good', 'Normal', 'Bad', 'Very bad'
    if not df_pollutant.empty and 'Item name' in df_pollutant.columns:
        df_pm25 = df_pollutant[df_pollutant['Item name'].str.contains("PM2.5", case=False, na=False)]
        if not df_pm25.empty:
            good_count = int(df_pm25['Good'].sum())
            normal_count = int(df_pm25['Normal'].sum())
            bad_count = int(df_pm25['Bad'].sum())
            very_bad_count = int(df_pm25['Very bad'].sum())
            q6 = {
                "Good": good_count,
                "Normal": normal_count,
                "Bad": bad_count,
                "Very bad": very_bad_count
            }
        else:
            q6 = {"Good": 0, "Normal": 0, "Bad": 0, "Very bad": 0}
    else:
        q6 = {"Good": 0, "Normal": 0, "Bad": 0, "Very bad": 0}

    result = {
        "target": {
            "Q1": q1,
            "Q2": q2,
            "Q3": q3,
            "Q4": q4,
            "Q5": q5,
            "Q6": q6
        }
    }
    return result


def main():
    result = compute_task1()
    if result is not None:
        # Write the output to predictions/questions.json
        output_path = os.path.join(BASE_DIR, "predictions/questions.json")
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        print("questions.json file has been created with the following content:")
        print(json.dumps(result, indent=2))
    else:
        print("Failed to compute task 1 results.")


if __name__ == "__main__":
    main()