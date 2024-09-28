import yfinance as yf
import pandas as pd
import yaml
import os

def load_yaml_config(file_path):
    """
    Loads a YAML configuration file.
    
    Args:
    file_path (str): The path to the YAML file.
    
    Returns:
    dict: A dictionary with the YAML configuration data.
    """
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def fetch_stock_data(stocks, start_date, end_date):
    """
    Fetches daily stock data (Open, High, Low, Close, Volume) from Yahoo Finance
    for a given list of stocks and a specified date range.
    
    Args:
    stocks (list): List of stock symbols to fetch data for.
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.
    
    Returns:
    pd.DataFrame: A DataFrame with Date, Stock, Open, High, Low, Close, and Volume columns.
    """
    all_data = []
    
    # Loop through each stock and fetch data
    for stock in stocks:
        stock_data = yf.download(stock, start=start_date, end=end_date)
        stock_data['Stock'] = stock
        stock_data.reset_index(inplace=True)  # Reset index to have Date as a column
        stock_data = stock_data[['Date', 'Stock', 'Open', 'High', 'Low', 'Close', 'Volume']]  # Select relevant columns
        all_data.append(stock_data)
    
    # Concatenate all stock data into a single DataFrame
    final_df = pd.concat(all_data, ignore_index=True)
    
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
    
    # Get the start_date, end_date, and stock_symbols from the config files
    start_date = general_config['start_date']
    end_date = general_config['end_date']
    stock_symbols = instrument_list['stock_symbols']
    
    # Fetch stock data
    stock_df = fetch_stock_data(stock_symbols, start_date, end_date)
    
    # Show the first few rows of the dataframe
    print(stock_df.head())
    
    # Optionally save the dataframe to a CSV file
    stock_df.to_csv('financial_data.csv', index=False)
