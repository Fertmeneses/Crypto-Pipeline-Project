# main1.py
# Main code to make pull requests to the CoinGecko API and download information to local files.

# Import libraries:
import requests
import argparse
import json
import os
import stat
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# WARNING: if you want to run the script directly and not using docker, 
# ... then uncomment the following lines

# from dotenv import load_dotenv # (pip install if not installed)
# load_dotenv("../../.env")

# Get API key:
API_KEY = os.getenv("COINGECKO_API_KEY")

# Check if key was loaded successfully. If not, interrupt the program:
if API_KEY:
    print("✅ API key loaded successfully.")
else:
    print("❌ Code interreputed: API key not found. Please check your environment setup.")
    sys.exit(1)

# Logging setup:
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Ensure output folder exists:
os.makedirs("crypto_datafiles", exist_ok=True)

def iso_to_coingecko_date(
    iso_date_str
    ):
    """
    Convert ISO8601 date 'YYYY-MM-DD' to 'dd-mm-yyyy' format required by the CoinGecko API
    and validate that it falls within the last 365 days (and is not in the future).
    --- Inputs ---
    {iso_date_str} [string]: Date in 'YYYY-MM-DD' format to be converted and validated.

    --- Returns ---
    formatted_date [string]: Date in 'dd-mm-yyyy' format (for CoinGecko API requests).
    filename_date  [string]: Date in 'YYYY_MM_DD' format (safe for filenames).

    --- Raises ---
    ValueError: If the input date format is invalid or if the date is outside the allowed range.
    """
    try:
        dt = datetime.strptime(iso_date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError("❌ Invalid date format. Use YYYY-MM-DD")

    today = datetime.today()
    one_year_ago = today - timedelta(days=365)

    if dt < one_year_ago or dt > today:
        raise ValueError("❌ Date must be within the last 365 days (and not in the future)")

    return dt.strftime("%d-%m-%Y"), dt.strftime("%Y_%m_%d")

def fetch_and_save(
    coin_id, 
    iso_date_str,
    max_attempts=5,
    wait=5
    ):
    """
    Fetch historical cryptocurrency data from the CoinGecko API for a specific coin and date, 
    then save the JSON response to a local file.

    The function validates the date, sends an HTTP GET request to the CoinGecko 
    `/coins/{id}/history` endpoint, and retries the request up to `max_attempts` times
    in case of failure, with a delay between attempts.

    --- Inputs ---
    {coin_id} [string]: The cryptocurrency ID used by CoinGecko.
    {iso_date_str} [string]: Date in ISO8601 'YYYY-MM-DD' format to request data for.
    {max_attempts} [int]: Maximum number of attempts before giving up.
    {wait} [int]: Base wait time in seconds before retrying. An extra 10 seconds
    per attempt number is added to reduce API rate-limit issues.

    --- Returns ---
    None

    --- Raises ---
    ValueError: If the input date is invalid or outside the allowed range (via iso_to_coingecko_date).
    Requests-related exceptions: If networking or HTTP issues occur outside handled retries.
    """
    # Check provided date is valid:
    try:
        formatted_date, filename_date = iso_to_coingecko_date(iso_date_str)
    except ValueError as e:
        logging.warning(f"{iso_date_str} skipped: {e}")
        return

    # Define request and parameters:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    params = {"date": formatted_date}
    headers = {"x-cg-demo-api-key": API_KEY}

    i_attempt = 1 # Initialize attempt counter
    while i_attempt <= max_attempts:
        # Make request:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        # Successful request, save the file locally:
        if response.status_code == 200:
            data = response.json()
            filename = f"crypto_datafiles/{coin_id}_{filename_date}.json"
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)

            # Set file permission: read/write for all (chmod 0666)
            os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
            logging.info(f"✅ Saved: {filename}")
            break # Exit the while loop
        # Failed request, re-attempt if allowed, else skip:
        else:
            logging.error(f"❌ Failed for {iso_date_str}: {response.status_code}")
            if i_attempt <=max_attempts:
                logging.info(f'Attempt failed, will try again. Remaining attempts: {max_attempts-i_attempt}')
            i_attempt += 1 # Update attempt counter
            time.sleep(wait+i_attempt*10)

def run_bulk(
    coin_id, 
    start_date, 
    end_date, 
    max_workers=1
    ):
    """
    Download and save historical cryptocurrency data from the CoinGecko API 
    for a given date range, using concurrent requests.

    --- Inputs ---
    {coin_id} [string]: The cryptocurrency ID used by CoinGecko (e.g. 'bitcoin').
    {start_date} [string]: Start date in ISO8601 'YYYY-MM-DD' format.
    {end_date} [string]: End date in ISO8601 'YYYY-MM-DD' format.
    {max_workers} [int]: Maximum number of concurrent threads for fetching data .

    --- Returns ---
    None

    --- Raises ---
    ValueError: If start_date or end_date have invalid format (raised indirectly 
    by datetime.strptime in `fetch_and_save`).
    """
    # Define time interval:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = (end_dt - start_dt).days + 1 # Time span, in days
    # If invalid interval, exit code:
    if delta <= 0:
        logging.error("❌ Invalid date range.")
        return
    # Define list for all available dates:
    date_list = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta)]

    # Start bulk processing:
    logging.info(f"🔁 Bulk processing {len(date_list)} days for '{coin_id}' with max {max_workers} workers")
    # Create thread pool for concurrent execution:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit one fetch task per date to the executor:
        futures = {
            executor.submit(fetch_and_save, coin_id, date): date
            for date in date_list
        }
        # Process completed tasks as they finish (tqdm for progress display):
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Fetching {coin_id}"):
            date = futures[future]
            try:
                # Get the result of the completed task (will raise if an exception occurred inside fetch_and_save)
                future.result()
            except Exception as e:
                # Log any error that happened during the fetch for this date
                logging.error(f"⚠️ Error processing {date}: {e}")

# Main function:

if __name__ == "__main__":
    # Define command-line interface (CLI) arguments:
    parser = argparse.ArgumentParser(description="CoinGecko Historical Downloader")
    parser.add_argument("coin", help="Coin ID (e.g. bitcoin, ethereum, cardano)")
    parser.add_argument("date", nargs="?", help="Date in YYYY-MM-DD")
    parser.add_argument("--bulk", action="store_true", help="Run bulk mode")
    parser.add_argument("--start", help="Start date for bulk mode (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date for bulk mode (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=1, help="Max concurrent workers (default: 1)")
    # Parse the CLI arguments:
    args = parser.parse_args()

    # Ensure output folder for downloaded data exists:
    os.makedirs("crypto_datafiles", exist_ok=True)

    # If bulk mode is enabled:
    if args.bulk:
        # Check that both start and end dates are provided:
        if not args.start or not args.end:
            logging.error("❌ Bulk mode requires --start and --end.")
        else:
            # Run bulk download over the specified date range:
            run_bulk(args.coin, args.start, args.end, max_workers=args.workers)
    else:
        # Single-date mode: ensure date is provided:
        if not args.date:
            logging.error("❌ Provide a date in YYYY-MM-DD format.")
        else:
            # Download and save data for the specified date:
            fetch_and_save(args.coin, args.date)