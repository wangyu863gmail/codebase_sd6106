import pandas as pd
import yaml
import os
from data_collection_fdata import fetch_stock_data
from data_collection_sdata import fetch_sentiment_data
import mysql.connector
from sqlalchemy import create_engine

###
def connect_to_mysql(i_user, i_password):
    """
    Connects to a MySQL database.
    
    Returns:
    mysql.connector.connection.MySQLConnection: A MySQL database connection.
    mysql.connector.cursor.MySQLCursor: A MySQL database cursor.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user=i_user,
        password=i_password
    )
    cursor = conn.cursor()
    
    return conn, cursor

def create_database(cursor, db_name):
    """
    Creates a new database in MySQL.
    
    Args:
    cursor (mysql.connector.cursor.MySQLCursor): A MySQL database cursor.
    db_name (str): The name of the database to create.
    """
    # Create the database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

def connect_to_database(conn, db_name):
    # Connect to the new QT_DB database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="121212zzZ",
        database=db_name
    )
    cursor = conn.cursor()

    return conn, cursor

def create_tables(cursor):
    # Create table_NP (Non-Partitioned Table)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS table_np (
        Date VARCHAR(10),
        Stock VARCHAR(10),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume BIGINT,
        OverallSentimentScore FLOAT
    )
    """)

    # Create an initial partitioned table without specific partitions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS table_p (
        Date VARCHAR(10),
        Stock VARCHAR(10),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume BIGINT,
        OverallSentimentScore FLOAT
    ) PARTITION BY LIST COLUMNS (Date)
    (
        PARTITION p_2024_09_29 VALUES IN ('2024-09-29'),
        PARTITION p_2024_09_30 VALUES IN ('2024-09-30')
    )
    """)

def add_partition_for_day(cursor, date_str):
    """
    Dynamically adds a partition for a specific date string.

    Args:
    cursor (mysql.connector.cursor.MySQLCursor): A MySQL database cursor.
    date_str (str): Date string in 'YYYY-MM-DD' format to create a partition.
    """
    # Replace dashes in the date string to create a valid partition name
    date_partition_name = date_str.replace('-', '_')
    
    # Dynamically create partition for the specific date
    cursor.execute(f"""
    ALTER TABLE table_P 
    ADD PARTITION (PARTITION p_{date_partition_name} VALUES IN ('{date_str}'))
    """)

def database_setup(user, password, database):
    # Connect to MySQL
    conn, cursor = connect_to_mysql(user, password)

    # Create the new database
    create_database(cursor, database)

    # Close the connection to the default database
    conn.close()

    # Connect to the new QT_DB database
    conn, cursor = connect_to_database(conn, database)

    # Create the tables
    create_tables(cursor)

    # Close the connection
    conn.close()

# In the load_data function, update the insert query
def load_data(df, user, password, database):
    conn, cursor = connect_to_mysql(user, password)
    conn, cursor = connect_to_database(conn, database)
    # Create SQLAlchemy engine
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@localhost/{database}')

    # Load data into table_NP
    df.to_sql('table_NP', engine, if_exists='append', index=False)

    # Ensure table_P is partitioned by date strings
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS table_P (
        Date VARCHAR(10),
        Stock VARCHAR(10),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume BIGINT,
        OverallSentimentScore FLOAT
    ) PARTITION BY LIST COLUMNS (Date)
    (
        PARTITION p_2024_09_29 VALUES IN ('2024-09-29'),
        PARTITION p_2024_09_30 VALUES IN ('2024-09-30')
    )
    """)

    # Get unique dates from the dataframe
    unique_dates = df['Date'].unique()

    print(df.info())

    # Create partitions for each unique date string
    for date_str in unique_dates:
        add_partition_for_day(cursor, date_str)

    # Insert data into table_P
    insert_query = """
    INSERT INTO table_P (Date, Stock, Open, High, Low, Close, Volume, OverallSentimentScore)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)  -- Note the use of backticks
    """
    
    data_to_insert = df.values.tolist()
    cursor.executemany(insert_query, data_to_insert)

    # Commit changes and close connection
    conn.commit()
    conn.close()


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

    mysql_user = general_config['user']
    mysql_password = general_config['password']
    mysql_database = general_config['database']

    stock_symbols = instrument_list['stock_symbols']
    
    # Fetch stock data
    #financial_df = fetch_stock_data(stock_symbols, start_date, end_date)
    #sentiment_df = fetch_sentiment_data(stock_symbols, start_date, end_date)

    financial_df = pd.read_csv(f'financial_data_{start_date}_{end_date}.csv')
    sentiment_df = pd.read_csv(f'sentiment_data_{start_date}_{end_date}.csv')

    final_df = pd.merge(financial_df, sentiment_df, on=['Date', 'Stock'], how='left')
    final_df.to_csv(f'final_data_{start_date}_{end_date}.csv', index=False)
#    
#    # Show the first few rows of the dataframe
#    print('financial data')
#    print(financial_df.head())
#
#    print('sentiment data')
#    print(sentiment_df.head())
#
#    # Show the first few rows of the dataframe
#    print('final_df data')
#    print(final_df.head())    

    start_date = general_config['start_date']
    end_date = general_config['end_date']

    database_setup(mysql_user, mysql_password, mysql_database)

    load_data(final_df, mysql_user, mysql_password, mysql_database)