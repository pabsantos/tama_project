from rain_data import load_rain_data, create_pcd_data, calc_daily_rain
import os
import pandas as pd


def main() -> None:
    ##-- rain data --##

    rain_data_path = "data/rain.parquet"
    rain_files_path = "data/ped_rain/"

    print(f"Loading rain csv files from '{rain_files_path}'...")
    full_df = load_rain_data(rain_files_path)

    if os.path.exists(rain_data_path):
        print(f"'{rain_data_path}' already exists, loading parquet...")
        rain_df = pd.read_parquet(rain_data_path)
    else:
        print(f"Calculating from '{rain_files_path}'")
        rain_df = calc_daily_rain(full_df)
        rain_df.to_parquet(path=rain_data_path)

    print("Loading pcd_data")
    pcd_data = create_pcd_data(full_df)


if __name__ == "__main__":
    main()
