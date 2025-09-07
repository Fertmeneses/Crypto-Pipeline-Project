# assign_trend_variance.py
# Assign trend and variance for each coin, daily.

import os
import pandas as pd
import psycopg2
import argparse
from dotenv import load_dotenv

from helper_functions import get_data_from_postgres, add_trend_and_variance_to_df, plot_trend

# Get environmental variables:
load_dotenv("../../.env")
PASSWORD = os.getenv("POSTGRES_PASSWORD") # Postgres password

if __name__ == "__main__":
	# Define command-line interface (CLI) arguments:
	parser = argparse.ArgumentParser(description="Assign trend and variance for each coin, daily")
	parser.add_argument("--table", type=str, help="Table name: crypto_daily_data (default) or coin_data")
	parser.add_argument("--trend", type=str, help="Trending criterion, either slope (default) or compare_extremes")
	parser.add_argument("--window", type=int, help="Time window to look back and calculate trend and variance, in days, default: 7")
	parser.add_argument("--frac", type=float, help="Tolerance for trend criterion, a fraction of the current price, default: 0.05")
	parser.add_argument("--plot_coin", type=str, help="Cryptocurreny to show graphical results: bitcoin (default), cardano or ethereum")
	parser.add_argument("--save_image", type=bool, help="Save image condition (default: False)")

	# Parse the CLI arguments:
	args = parser.parse_args()

	# If table is provided:
	table = args.table if args.table else 'crypto_daily_data'
	# If trend is provided:
	trend = args.trend if args.trend else 'slope'
	# If window is provided:
	window = args.window if args.window else 7
	# If frac is provided:
	frac = args.frac if args.frac else 0.05
	# If plot_coin is provided:
	plot_coin = args.plot_coin if args.plot_coin else 'bitcoin'
	# If save_image is provided:
	save_image = args.save_image if args.save_image else False

	# Get information as dataframe:
	df = get_data_from_postgres(password=PASSWORD,table=table)

	# Assign trend and variance:
	df_trend_var = add_trend_and_variance_to_df(
		df,trend_method=trend,window_back_days=window,fraction_criterion=frac)

	# Display in screen
	print(df_trend_var.head(20))

	# Plot figure for the chosen coin, for the entire time range in data:
	plot_trend(df_trend_var,trend,window,frac,coin=plot_coin,save_image=save_image)