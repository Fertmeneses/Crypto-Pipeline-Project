# make_ML_predictions.py
# Make ML predictions for cryptocurrency prices

import os
import pandas as pd
import psycopg2
import argparse
from dotenv import load_dotenv

from helper_functions import (
	get_data_from_postgres, apply_transformation_to_orig_df,
	train_per_coin_models_LinearRegression, plot_predictions,
	train_per_coin_rf_models)

# Get environmental variables:
load_dotenv("../../.env")
PASSWORD = os.getenv("POSTGRES_PASSWORD") # Postgres password

if __name__ == "__main__":
	# Define command-line interface (CLI) arguments:
	parser = argparse.ArgumentParser(description="Apply feature engineering to daily datasets")
	parser.add_argument("--table", type=str, help="Table name: crypto_daily_data (default) or coin_data")
	parser.add_argument("--apply_risk", action="store_true", help="Assign risks")
	parser.add_argument("--risk_streak_days", type=int, help="Number of dropping streak days for risk assignment (default: 1)")
	parser.add_argument("--risk_period_days", type=int, help="Number of days for the risk period (default: 30)")
	parser.add_argument("--apply_trend_var", action="store_true", help="Assign trend and variance")
	parser.add_argument("--trend_method", type=str, help="Trending criterion, either slope (default) or compare_extremes")
	parser.add_argument("--trend_var_window", type=int, help="Time window to look back and calculate trend and variance, in days, default: 7")
	parser.add_argument("--trend_frac", type=float, help="Tolerance for trend criterion, a fraction of the current price, default: 0.05")
	parser.add_argument("--apply_lagged_prices", action="store_true", help="Create lagged prices")
	parser.add_argument("--apply_calendar_features", action="store_true", help="Assign weekend/week days, holidays/normal days in US and China")
	parser.add_argument("--apply_risk_mapping", action="store_true", help="Apply risk mapping transformation")
	parser.add_argument("--apply_price_normalization", action="store_true", help="Apply lagged-prices normalization")
	parser.add_argument("--allow_ML_Linear_Model", action="store_true", help="Allow to train and evaluate a linear regression model")
	parser.add_argument("--allow_ML_RF_Model", action="store_true", help="Allow to train and evaluate a Random Forest Regressor model")
	parser.add_argument("--RF_n_estimators", type=int, help="Number of estimators for the RF model default: 500")
	parser.add_argument("--RF_max_depth", type=int, help="Max depth for the RF model default: 10")
	parser.add_argument("--save_image", type=bool, help="Save image condition (default: False)")
	
	# Parse the CLI arguments:
	args = parser.parse_args()

	# Set variables for source table:
	table = args.table if args.table else 'crypto_daily_data'

	# Set variables for risk assignments:
	apply_risk = args.apply_risk if args.apply_risk else False
	risk_streak_days = args.risk_streak_days if args.risk_streak_days else 1
	risk_period_days = args.risk_period_days if args.risk_period_days else 30
	apply_risk_mapping = args.apply_risk_mapping if args.apply_risk_mapping else False

	# Set variables for trend and variance assignments:
	apply_trend_var = args.apply_trend_var if args.apply_trend_var else False
	trend_method = args.trend_method if args.trend_method else 'slope'
	trend_var_window = args.trend_var_window if args.trend_var_window else 7
	trend_frac = args.trend_frac if args.trend_frac else 0.05	

	# Set variables for lagged prices:
	apply_lagged_prices = args.apply_lagged_prices if args.apply_lagged_prices else False
	apply_price_normalization = args.apply_price_normalization if args.apply_price_normalization else False

	# Set variables for calendar features:
	apply_calendar_features = args.apply_calendar_features if args.apply_calendar_features else False

	# Set variables for ML training:
	allow_ML_Linear_Model = args.allow_ML_Linear_Model if args.allow_ML_Linear_Model else False
	allow_ML_RF_Model = args.allow_ML_RF_Model if args.allow_ML_RF_Model else False
	RF_n_estimators = args.RF_n_estimators if args.RF_n_estimators else 500
	RF_max_depth = args.RF_max_depth if args.RF_max_depth else 10

	# If save_image is provided:
	save_image = args.save_image if args.save_image else False		

	# Get information as dataframe:
	df = get_data_from_postgres(password=PASSWORD,table=table)

	# Apply transformations
	df_full = apply_transformation_to_orig_df(
		df,
		apply_risk=apply_risk,
		risk_streak_days=risk_streak_days,
		risk_period_days=risk_period_days,
		apply_trend_var=apply_trend_var,
		trend_method=trend_method,
		trend_var_window=trend_var_window,
		trend_frac=trend_frac,
		apply_lagged_prices=apply_lagged_prices,
		apply_calendar_features=apply_calendar_features,
		apply_riks_mapping=apply_risk_mapping,
		apply_price_normalization=apply_price_normalization
		)

	# Drop rows that contain NaN values (the first rows with not enough information)
	df_full = df_full.dropna()

	# Train and evaluate ML Linear Regression model:
	if allow_ML_Linear_Model:
		models_LinReg, results_df_LinReg = train_per_coin_models_LinearRegression(df_full)
		plot_predictions(df_full,models_LinReg,save_image=save_image)

	if allow_ML_RF_Model:
		models_RF, results_df_RF = train_per_coin_rf_models(
			df_full,n_estimators=RF_n_estimators,max_depth=RF_max_depth)
		plot_predictions(df_full,models_RF,save_image=save_image)