# assign_risk.py
# Assign a risk type for each month based on a dropping-streak criterion.

import os
import pandas as pd
import psycopg2
import argparse
from dotenv import load_dotenv

from helper_functions import get_data_from_postgres, add_risks_to_df

# Get environmental variables:
load_dotenv("../../.env")
PASSWORD = os.getenv("POSTGRES_PASSWORD") # Postgres password

if __name__ == "__main__":
	# Define command-line interface (CLI) arguments:
	parser = argparse.ArgumentParser(description="Assign a risk type for each month based on a dropping-streak criterion")
	parser.add_argument("--table", type=str, help="Table name: crypto_daily_data (default) or coin_data")
	parser.add_argument("--streak_days", type=int, help="Number of dropping streak days for risk assignment (default: 1)")
	parser.add_argument("--risk_period_days", type=int, help="Number of days for the risk period (default: 30)")

	# Parse the CLI arguments:
	args = parser.parse_args()

	# If table is provided:
	table = args.table if args.table else 'crypto_daily_data'
	# If streak_days is provided:
	streak_days = args.streak_days if args.streak_days else 1
	# If risk_period_days is provided:
	risk_period_days = args.risk_period_days if args.risk_period_days else 30

	# Get information as dataframe:
	df = get_data_from_postgres(password=PASSWORD,table=table)

	# Assign risks:
	df_risks = add_risks_to_df(df,drop_streak_days=streak_days,risk_period_days=risk_period_days)

	# Display in screen
	print(df_risks.iloc[risk_period_days:risk_period_days+30])
