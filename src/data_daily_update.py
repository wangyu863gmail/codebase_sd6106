import pandas as pd
import yaml
import os
from data_collection_fdata import fetch_stock_data
from data_collection_sdata import fetch_sentiment_data
from data_collection import load_yaml_config, connect_to_mysql, connect_to_database, load_data
from sqlalchemy import create_engine, text, exc
from datetime import datetime, timedelta

def fetch_daily_and_merge_data(stock_symbols, start_date, end_date):

#    financial_df = fetch_stock_data(stock_symbols, start_date, end_date)
#    sentiment_df = fetch_sentiment_data(stock_symbols, start_date, end_date) 
#
#    filename = f'financial_data_{start_date}_{end_date}.csv'
#    financial_df.to_csv(filename, index=False)
#
#    # Optionally save the dataframe to a CSV file
#    filename = f'sentiment_data_{start_date}_{end_date}.csv'
#    sentiment_df.to_csv(filename, index=False)   

    financial_df = pd.read_csv(f'financial_data_{start_date}_{end_date}.csv')
    sentiment_df = pd.read_csv(f'sentiment_data_{start_date}_{end_date}.csv')       

    if not pd.api.types.is_datetime64_any_dtype(financial_df['Date']):
        financial_df['Date'] = pd.to_datetime(financial_df['Date'])
    financial_df['Date'] = financial_df['Date'].dt.strftime('%Y-%m-%d')
    final_df = pd.merge(financial_df, sentiment_df, on=['Date', 'Stock'], how='left')
    filename = f'final_data_{start_date}_{end_date}.csv'
    final_df.to_csv(filename, index=False)

    return final_df   

if __name__ == "__main__":
    # Define the config folder path
    conf_folder = os.path.join(os.getcwd(), 'conf')
    
    # Load the general config (dates) from YAML file
    general_config_path = os.path.join(conf_folder, 'general_config.yml')
    general_config = load_yaml_config(general_config_path)
    
    # Load the stock symbols from YAML file
    instrument_list_path = os.path.join(conf_folder, 'instrument_list.yml')
    instrument_list = load_yaml_config(instrument_list_path)    

#    # Get today's date
#    today = datetime.now()    
#
#    # Format today's date as YYYY-MM-DD and convert to string
#    start_date = today.strftime('%Y-%m-%d')  # Start date (today)
#    end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')  # End date (tomorrow)
#
#    # Ensure both are strings
#    start_date = str(start_date)  # Start date as string
#    end_date = str(end_date)        # End date as string

    start_date = general_config['today_start_date']
    end_date = general_config['today_end_date']

    mysql_user = general_config['user']
    mysql_password = general_config['password']
    mysql_database = general_config['database']

    stock_symbols = instrument_list['stock_symbols']    

    daily_final_df = fetch_daily_and_merge_data(stock_symbols, start_date, end_date)

    load_data(daily_final_df, mysql_user, mysql_password, mysql_database)