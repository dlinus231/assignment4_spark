import os
import urllib.request
import ssl

# Disable strict SSL certificate verification to bypass LinuxLab cert issues
ssl._create_default_https_context = ssl._create_unverified_context

def download_file(url, destination):
    """
    Downloads a file from a URL to a local destination.
    This function is idempotent: if the file already exists, it will be safely overwritten.
    """
    # Ensure the target directory exists. exist_ok=True prevents errors if it's already there.
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    print(f"Downloading {url} to {destination}...")

    # urllib.request.urlretrieve naturally overwrites the destination file,
    # ensuring we don't end up with duplicate appended data (e.g., data.csv, data(1).csv)
    urllib.request.urlretrieve(url, destination)

    print(f"Successfully downloaded and saved to {destination}.")

if __name__ == "__main__":
    print("Starting data download process...")

    # Define the data directory
    DATA_DIR = "data"

    # 1. Download the Taxi Zone Lookup CSV
    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    zone_dest = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
    download_file(zone_url, zone_dest)

    # 2. Download the 2023 Yellow Taxi Trip Data (Example: Just Jan & Feb to save time/space)
    # To download the full year, you could loop through range(1, 13)
    for month in range(1, 13):
        month_str = str(month).zfill(2) # Formats 1 as '01'
        parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month_str}.parquet"
        parquet_dest = os.path.join(DATA_DIR, f"yellow_tripdata_2023-{month_str}.parquet")

        try:
            download_file(parquet_url, parquet_dest)
        except Exception as e:
            print(f"Failed to download month {month_str}: {e}")

    print("All downloads completed!")
