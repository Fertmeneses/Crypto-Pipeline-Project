# helper_functions.py
# Helper functions for cryptocurrency analysis

import psycopg2
import pandas as pd
import numpy as np
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import calendar
import holidays
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor

# ==============

# Set global plot parameters:

COLOR_COINS = {
	"bitcoin":'teal',
	"ethereum":'navy',
	"cardano":'green'
}
SCATTER_COINS = {
	"bitcoin":'o',
	"ethereum":'s',
	"cardano":'d'
}
LINE_COINS = {
	"bitcoin":'-',
	"ethereum":'--',
	"cardano":':'
}

# ==============

def get_data_from_postgres(
	host='127.0.0.1',
	port=5432,
	dbname='postgres',
	user='postgres',
	password='',
	table='crypto_daily_data'
	):
	"""
	XXxx
	"""
	# Set connection details for information request:
	db_params = {
		"host": host,
		"port": port,
		"dbname": dbname,
		"user": user,
		"password": password
	}

	# Set table variables:
	if table=='crypto_daily_data':
		coin_var = 'coin_id'
		price_var = 'price_usd'
	elif table=='coin_data':
		coin_var = 'coin'
		price_var = 'price'
	else:
		print("❌ Choose a valid table: either 'crypto_daily_data' or 'coin_data'.")
		sys.exit(1)

	# Define SQL query:
	SQL_query = f"""
		SELECT
		{coin_var} AS coin_id,
		date,
		{price_var} AS price_usd
		FROM {table}
		"""

	# Connect, run query and get dataframe:
	with psycopg2.connect(**db_params) as conn:
		df = pd.read_sql(SQL_query, conn)

	# Convert date from string to datetime:
	df['date'] = pd.to_datetime(df['date'])

	return df

# ==============

def plot_recent_history(
	df,
	last_N_days=30,
	coins=['bitcoin','ethereum','cardano'],
	last_date='latest',
	figsize=(8,6),
	save_image=True,
	):
	"""
	Plot prices of cryptocurreny for the last N days since a chosen date.
	"""
	# Select last date:
	if last_date == 'latest':
		max_date = df['date'].max()
	else:
		max_date = datetime.strptime(last_date, '%Y-%m-%d')

	# Calculate initial date:
	cutoff_date = max_date - pd.Timedelta(days=last_N_days)

	# Filter dataset to the selected time period:
	df_last_N = df[(df['date'] > cutoff_date) & (df['date'] <= max_date)].sort_values('date')

	# Plot figure:
	fig = plt.figure(figsize=figsize)
	for i,coin in enumerate(coins):
	    df_coin = df_last_N[df_last_N['coin_id']==coin]
	    plt.plot(
	    	df_coin['date'],df_coin['price_usd'],alpha=0.8,label=coin,
	    	color=COLOR_COINS[coin],lw=2-0.5*i,ls=LINE_COINS[coin])
	plt.title(f'Cryptocurrency prices: last {last_N_days} days since {max_date.date()}')
	plt.xlabel('Dates')
	plt.ylabel('Price [USD]')
	plt.xticks(rotation=45)
	plt.grid(True, axis='y', lw=0.5, alpha=0.5)
	plt.legend(title='Cryptocoin')
	fig.tight_layout()
	if save_image:
		coins_name = "_".join(coins) 
		fig_name = f"{coins_name}_prices_last_{last_N_days}_days_since_{max_date.date()}.png"
		plt.savefig(f'Images/{fig_name}')
		print(f'Image saved: "Images/{fig_name}"')
	plt.show()

# ==============

def get_month_df(
	df, 
	month
	):
	"""
	XXxx
	"""
	# Ensure date is datetime
	df['date'] = pd.to_datetime(df['date'])

	# If month is a string, convert to number
	if isinstance(month, str):
		month = month.strip().lower()
		try:
			month_num = list(calendar.month_name).index(month.capitalize())
		except ValueError:
			month_num = list(calendar.month_abbr).index(month.capitalize())
	else:
		month_num = int(month)

	# Filter DataFrame
	dfm = df[df['date'].dt.month == month_num].copy()

	return dfm

# ==============

def add_risks_to_df(
	dfm,
	drop_streak_days=1,
	risk_period_days=30
	):
	"""
	Xxx
	"""
	# Copy input dataframe and sort values:
	dfm = dfm.sort_values(['coin_id', 'date'])

	# Evaluate risks for each coin separately:
	coin_risk = [] # Initiate
	for coin, df_coin in dfm.groupby('coin_id',sort=False):
		# Copy dataframe and set date as index:
		df_coin = df_coin.copy().set_index('date')
		# Get daily percentual change:
		df_coin['pct_change'] = df_coin['price_usd'].pct_change() * 100

		# Check if there was a 50% or more drop for consecutive days:
		drop50 = (df_coin['pct_change'] <= -50)
		# Check if there was between 20% and 50% drop for consecutive days:
		drop20_50 = ((df_coin['pct_change'] <= -20) & (df_coin['pct_change'] > -50))

		# Count the cumulative days in drop streaks, below is an example of how it works:
		run50 = drop50.groupby((~drop50).cumsum()).cumsum()
		run20 = drop20_50.groupby((~drop20_50).cumsum()).cumsum()
		# Example for run50:
		# drop50:  T   T   F   T   T   T   F
		# groups:  0   0   1   1   1   1   2      # (~drop50).cumsum()
		# run50:   1   2   0   1   2   3   0      # cumsum within each group

		# Check if there is at least a {drop_streak_days}-day in the series:
		cond50 = run50.ge(drop_streak_days)
		cond20 = run20.ge(drop_streak_days)

		# Use a time-based rolling window over the previous {risk_period_days}:
		# rolling(...).max() is True if ANY day in the window satisfied cond50 / cond20.
		# shift(1) excludes "today" from the window.
		had50_prior = cond50.rolling(f'{risk_period_days}D').max().shift(1).fillna(False).astype(bool)
		had20_prior = cond20.rolling(f'{risk_period_days}D').max().shift(1).fillna(False).astype(bool)

		# Assign precedence High > Medium > Low
		risk = pd.Series('Low', index=df_coin.index) # All are Low by default
		risk = risk.mask(had20_prior, 'Medium') # Updated to Medium if conditions are met
		risk = risk.mask(had50_prior, 'High') # Updated to High if conditions are met

		# Save the risks for this coin
		coin_risk.append(
			df_coin.assign(risk_level=risk.values) # Assign risk values
			.reset_index()[['coin_id', 'date', 'risk_level']] # Restore the indexes
		)

	# Combine per-coin risks and merge back to original dataframe:
	risk_all_coins = pd.concat(coin_risk, ignore_index=True)
	df_risk = dfm.merge(risk_all_coins, on=['coin_id', 'date'], how='left')

	return pd.DataFrame(df_risk)

# ==============

def slope_lin(a):
	"""
	XXxx
	"""
	x = np.arange(len(a), dtype=float) # Array of as many integers as days in the window
	y = a.astype(float) # Price values

	# Return slope in price units per day:
	return np.polyfit(x, y, 1)[0]

# ==============

def add_trend_and_variance_to_df(
    df,
    trend_method="slope", 
    window_back_days=7,
    fraction_criterion=0.05
	):
	"""
	Xxxx
	"""

	# Copy original dataframe:
	df_trend = df.copy()

	# Make sure dates are sorted in ascending order:
	df_trend = df_trend.sort_values(["coin_id", "date"])

	# Window length, including the current day:
	win = int(window_back_days) + 1

	# Calculate variance for each coin:
	df_trend["variance"] = (
		df_trend.groupby("coin_id")["price_usd"].transform(
			lambda s: s.rolling(win, min_periods=win).var())
	)

	# Analyze the general trend, according to the input criterion:
	if trend_method == "compare_extremes":
		# Compare the price at current day p0 vs price 7 days before (p-7) using a relative threshold:
		base = df_trend.groupby("coin_id")["price_usd"].shift(window_back_days) # p-7
		rel_diff = (df_trend["price_usd"] - base) / base  # Relative difference over the window
		# Assign trend category:
		df_trend["trend"] = np.select(
			[rel_diff >  fraction_criterion,
			np.abs(rel_diff) <= fraction_criterion,
			rel_diff < -fraction_criterion],
			["Rising", "Flat","Dropping"],
			default=None # This gives the Flat category to NaN values
		)

	elif trend_method == "slope":
		# Analyze the slope of the present and last 7 days using linear regression,
		# ... and assign trend category using the tolerance fraction:

		# Calculate the slope for rolling windows:
		slope_series = (
			df_trend.groupby("coin_id")["price_usd"].transform(
				lambda s: s.rolling(win, min_periods=win).apply(slope_lin, raw=True))
		)
		# Determine relative change compared to current price:
		rel_change = slope_series*win / df_trend["price_usd"]
		# Assign trend category:
		df_trend["trend"] = np.select(
			[rel_change >  fraction_criterion,
			np.abs(rel_change) <= fraction_criterion,
			rel_change < -fraction_criterion],
			["Rising", "Flat", "Dropping"],
			default=None
		)
	else:
		raise ValueError("trend_method must be 'slope' or 'compare_extremes'")

	return df_trend

# ==============

def plot_trend(
	df,
	trend_method,
	window,
    fraction_criterion,
	coin='bitcoin',
	figsize=(10,6),
	save_image=False
	):
	"""
	Xxxx
	"""
	# Filter the input dataset to the chosen coin:
	df_coin = df[df['coin_id']==coin]
	# Separate trends:
	df_coin_rising = df_coin[df_coin['trend']=='Rising']
	df_coin_flat = df_coin[df_coin['trend']=='Flat']
	df_coin_droping = df_coin[df_coin['trend']=='Dropping']

	# Plot figure:
	fig = plt.figure(figsize=figsize)

	plt.scatter(df_coin_rising['date'],df_coin_rising['price_usd'],alpha=0.8,
		label='Rising',marker='v')
	plt.scatter(df_coin_flat['date'],df_coin_flat['price_usd'],alpha=0.8,
		label='Flat',marker='p')
	plt.scatter(df_coin_droping['date'],df_coin_droping['price_usd'],alpha=0.8,
		label='Droping',marker='*')

	plt.title(f'Trend of {coin}, using "{trend_method}" method, {window}-day window and {fraction_criterion} fraction tolerance')
	plt.xlabel('Dates')
	plt.ylabel('Price [USD]')
	plt.xticks(rotation=45)
	plt.grid(True, axis='y', lw=0.5, alpha=0.5)
	plt.legend(title='General trend')
	fig.tight_layout()
	if save_image:
		fig_name = f"{coin}_trend_{trend_method}_window_{window}_days_tolerance_{fraction_criterion}.png"
		plt.savefig(f'Images/{fig_name}')
		print(f'Image saved: "Images/{fig_name}"')
	plt.show()

# ==============

def add_lagged_features(
    df, 
    target_col='price_usd',
    win=7
    ):
	"""
	Xxxx
	"""
	# Add lagged features:
	df_lagged = df.copy()
	for i in range(1, win + 1):
		df_lagged[f"{target_col}-{i}"] = df_lagged[target_col].shift(i)

	# Reorder columns: everything except target, then target, then lags
	lag_cols = [f"{target_col}-{i}" for i in range(1, win + 1)]
	other_cols = [c for c in df.columns if c != target_col]
	df_lagged = df_lagged[other_cols + [target_col] + lag_cols]

	return df_lagged

# ==============

def add_calendar_features(
	df
	):
	"""
	XXxxx
	"""
	# Make dataset copy:
	df_calendar = df.copy()

	# Add weekend flag (Saturday=5, Sunday=6):
	df_calendar['is_weekend'] = df_calendar['date'].dt.weekday.isin([5, 6]).astype(int)

	# Prepare holiday calendars:
	us_holidays = holidays.UnitedStates(years=df_calendar['date'].dt.year.unique())
	cn_holidays = holidays.China(years=df_calendar['date'].dt.year.unique())

	# Add holiday flags:
	df_calendar['is_US_holiday'] = df_calendar['date'].dt.date.isin(us_holidays).astype(int)
	df_calendar['is_China_holiday'] = df_calendar['date'].dt.date.isin(cn_holidays).astype(int)

	return df_calendar

# ==============

def map_risks_to_numbers(
	df,
	risk_col='risk_level',
	risk_map={'Low': 1, 'Medium': 2, 'High': 3}
	):
	"""
	XXxx
	"""
	df_mapped = df.copy()
	# Map risk levels to numbers (if not done yet):
	if df[risk_col].dtype == object:
	    df_mapped[risk_col] = df_mapped[risk_col].map(risk_map)

	return df_mapped

# ==============

def normalize_prices(
	df,
	lag_window,
	col_price_root='price_usd'
	):
	"""
	XXxx
	"""
	df_norm_prices = df.copy()
	# Keep the original yesterday price:
	df_norm_prices[f'{col_price_root}-1_orig'] = df[f'{col_price_root}-1']
	# Normalize the price features:
	for lag in range(1, lag_window+1):
		df_norm_prices[f'{col_price_root}-{lag}'] = df[f'{col_price_root}-{lag}'] / df[f'{col_price_root}-1']

	return df_norm_prices

# ==============

def apply_transformation_to_orig_df(
	df,
	apply_risk=True,
	risk_streak_days=1,
	risk_period_days=30,
	apply_trend_var=True,
	trend_method='slope',
	trend_var_window=7,
	trend_frac=0.05,
	apply_lagged_prices=True,
	apply_calendar_features=True,
	apply_riks_mapping=True,
	apply_price_normalization=True
	):
	"""
	XXxx
	"""
	df_full = df.copy()

	# Apply risk assignment:
	if apply_risk:
		df_full = add_risks_to_df(df_full,drop_streak_days=risk_streak_days,risk_period_days=risk_period_days)
		# Apply risk transformation from string to integers:
		if apply_riks_mapping:
			df_full = map_risks_to_numbers(df_full)

	# Apply trend and variance assignment:
	if apply_trend_var:
		df_full = add_trend_and_variance_to_df(
			df_full,trend_method=trend_method,window_back_days=trend_var_window,fraction_criterion=trend_frac)

	# Apply lagged prices:
	if apply_lagged_prices:
		df_full = add_lagged_features(df_full,win=7)
		# Apply price normalization:
		if apply_price_normalization:
			df_full = normalize_prices(df_full,7)

	# Apply calendar features:
	if apply_calendar_features:
		df_full = add_calendar_features(df_full)

	return df_full

# ==============

def split_train_test(
	df,
	train_frac=0.75
	):
	"""
	Split chronologically.
	"""
	split_idx = int(len(df) * train_frac)
	train_df = df.iloc[:split_idx]
	test_df = df.iloc[split_idx:]

	return train_df, test_df

# ==============

def check_drop_cols(
	drop_cols,
	extra_cols
	):
	"""
	--- Inputs ---
	{drop_cols} [list]
	{extra_cols} [list]
	"""
	if drop_cols is None:
		drop_cols = extra_cols
	else:
		drop_cols.extend(extra_cols)	

	return drop_cols

# ==============

def prepare_inputs_ML(
	train_df,
	test_df,
	drop_cols,
	target
	):
	"""
	"""
	# Select features for training:
	feature_cols = [c for c in train_df.columns if c not in drop_cols]
	# Organize predictors (X) and labels (y) for training and testing:
	X_train = train_df[feature_cols]
	y_train = train_df[target]
	X_test  = test_df[feature_cols]
	y_test  = test_df[target]

	return X_train, y_train, X_test, y_test

# ==============

def train_per_coin_models_LinearRegression(
    df, 
    target='price_usd', 
    ref_price='price_usd-1_orig',
    coin_col='coin_id',
    date_col='date',
    drop_cols = ['coin_id', 'date', 'risk_level', 'trend'],
    train_frac=0.75
    ):
    """
    Train a Linear Regression model per coin to predict T0 price based on past 7 days
    and other available features.
    """
    # Initialize model and results
    models = {}
    results = []

    # Features to drop (not used for training)
    drop_cols = check_drop_cols(drop_cols,[ref_price, target])
    
    # Train and evaluate a ML model for each coin:
    for coin, df_coin in df.groupby(coin_col):
        df_coin = df_coin.sort_values(date_col).reset_index(drop=True)
        
        # Split chronological:
        train_df, test_df = split_train_test(df_coin,train_frac=train_frac)
        
        # Define predictors (X) and targets (y) for train/test:
        X_train, y_train, X_test, y_test = prepare_inputs_ML(train_df,test_df,drop_cols,target)
       
        # Fit model
        model = LinearRegression()
        model.fit(X_train, y_train)
        models[coin] = model
        
        # Predict (normalized)
        y_pred_norm = model.predict(X_test)
        
        # Scale back to absolute prices
        y_pred_abs = y_pred_norm * test_df[ref_price].values
        y_test_abs = y_test * test_df[ref_price].values
        
        # Compute RMSE on absolute prices
        rmse = mean_squared_error(y_test_abs, y_pred_abs, squared=False)
        
        # Compute results
        results.append({'coin_id': coin, 'RMSE_abs_price': rmse})
    
    results_df = pd.DataFrame(results)
    return models, results_df

# ==============

def train_per_coin_rf_models(
    df, 
    target='price_usd', 
    ref_price='price_usd-1_orig',
    coin_col='coin_id',
    date_col='date',
    drop_cols = ['coin_id', 'date', 'risk_level', 'trend'],
    train_frac=0.75,
    n_estimators=500,
    max_depth=10,
    min_samples_leaf=1,
    random_state=17,
    n_jobs=-1
    ):
    """
    Train a Random Forest Regressor model per coin to predict T0 price based on past 7 days
    and other available features.
    """
    # Initialize model and results
    models = {}
    results = []

    # Features to drop (not used for training)
    drop_cols = check_drop_cols(drop_cols,[ref_price, target])
    
    # Train and evaluate a ML model for each coin:
    for coin, df_coin in df.groupby(coin_col):
        df_coin = df_coin.sort_values(date_col).reset_index(drop=True)
        
        # Split chronological:
        train_df, test_df = split_train_test(df_coin,train_frac=train_frac)
        
        # Define predictors (X) and targets (y) for train/test:
        X_train, y_train, X_test, y_test = prepare_inputs_ML(train_df,test_df,drop_cols,target)
       
        # Fit model
        rf = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            random_state=random_state,
            n_jobs=n_jobs
        )
        rf.fit(X_train, y_train)
        models[coin] = rf
        
        # Predict (normalized)
        y_pred_norm = rf.predict(X_test)
        
        # Scale back to absolute prices
        y_pred_abs = y_pred_norm * test_df[ref_price].values
        y_test_abs = y_test * test_df[ref_price].values
        
        # Compute RMSE on absolute prices
        rmse = mean_squared_error(y_test_abs, y_pred_abs, squared=False)
        
        # Compute results
        results.append({'coin_id': coin, 'RMSE_abs_price': rmse})
    
    results_df = pd.DataFrame(results)
    return models, results_df

# ==============

def plot_predictions(
    df, 
    models, 
    target='price_usd', 
    ref_price='price_usd-1_orig',
    coin_col='coin_id',
    date_col='date',
    drop_cols = ['coin_id', 'date', 'risk_level', 'trend'],
    coin_to_plot=None,
    train_frac=0.75,
    save_image=False
	):
    """
    Plot predictions vs ground truth for the test set of each coin (or a specific one).
    """
    
    # Features to drop:
    drop_cols = check_drop_cols(drop_cols,[ref_price, target])
    
    # Check coins to be analyzed:
    coins = [coin_to_plot] if coin_to_plot else list(models.keys())
    
    # Make predictions for every coin and plot:
    for coin in coins:
    	# Prepare test dataset:
        df_coin = df[df[coin_col] == coin].sort_values(date_col).reset_index(drop=True)
        split_idx = int(len(df_coin) * train_frac)
        test_df = df_coin.iloc[split_idx:]
        
        # Prepare predictors and targets:
        feature_cols = [c for c in df_coin.columns if c not in drop_cols]
        X_test = test_df[feature_cols]
        y_test = test_df[target]
        
        # Predict (normalized) and rescale
        y_pred_norm = models[coin].predict(X_test)
        y_pred_abs = y_pred_norm * test_df[ref_price].values
        y_test_abs = y_test * test_df[ref_price].values

        # Compute RMSE on absolute prices
        rmse = mean_squared_error(y_test_abs, y_pred_abs, squared=False)

        # Prepare ML model's name:
        model_name = str(models[coin]).split('(')[0] 

        # Plot
        fig = plt.figure(figsize=(10, 5))
        plt.plot(test_df['date'], y_test_abs, label='Ground Truth', 
        	lw=1, ls='--', alpha=0.7, color=COLOR_COINS[coin])
        plt.plot(test_df['date'], y_pred_abs, label='Prediction', lw=2, 
        	ls='-', alpha=0.9, color=COLOR_COINS[coin])
        plt.title(f'{coin} — Ground Truth vs Prediction — Model {model_name} — RMSE {rmse}')
        plt.xlabel('Date')
        plt.ylabel('Price [USD]')
        plt.legend()
        plt.grid(True, axis='y', alpha=0.5)
        plt.xticks(rotation=45)
        plt.tight_layout()
        fig.tight_layout()
        if save_image:
            plt.savefig(f'Images/Preds_{coin}_model_{model_name}')
        plt.show()

# ==============