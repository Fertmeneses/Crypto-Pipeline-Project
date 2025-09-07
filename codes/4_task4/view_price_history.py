# view_price_history.py
# View the history of chosen coins for the last N days from a given date.

import os
import pandas as pd
import psycopg2
import argparse
from dotenv import load_dotenv

from helper_functions import get_data_from_postgres, plot_recent_history

# Get environmental variables:
load_dotenv("../../.env")
PASSWORD = os.getenv("POSTGRES_PASSWORD") # Postgres password

if __name__ == "__main__":
	# Define command-line interface (CLI) arguments:
	parser = argparse.ArgumentParser(description="View data history for bitcoin, ethereum and cardano")
	parser.add_argument("--table", type=str, help="Table name: crypto_daily_data (default) or coin_data")
	parser.add_argument("--coins", nargs="+", help="Coins to analyze (space-separated). Leave off for all.")
	parser.add_argument("--last_date", type=str, help="Last date to retrieve information (default: latest)")
	parser.add_argument("--days", type=int, help="Number of days to look back (default: 30)")
	parser.add_argument("--save_image", type=bool, help="Save image condition (default: False)")
	# Parse the CLI arguments:
	args = parser.parse_args()

	# If table is provided:
	table = args.table if args.table else 'crypto_daily_data'
	# If coins is provided:
	coins = args.coins if args.coins else ['bitcoin','cardano','ethereum']
	# If last_date is provided:
	last_date = args.last_date if args.last_date else 'latest'
	# If days is provided:
	days = args.days if args.days else 30
	# If save_image is provided:
	save_image = args.save_image if args.save_image else False		

	# Get information as dataframe:
	df = get_data_from_postgres(password=PASSWORD,table=table)

	plot_recent_history(
		df,
		last_N_days=days,
		coins=coins,
		last_date=last_date,
		figsize=(8,6),
		save_image=save_image,
		)