import requests
import pandas as pd
from datetime import datetime, timedelta
from transformers import pipeline
import yaml
import os

# Initialize FinBERT for sentiment analysis
finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")

# Define the API key and the NewsAPI URL for news collection
#NEWS_API_KEY = '32e5e5f3d5734a5ea86c7f3fa4daea51'  # wangyu863
NEWS_API_KEY = 'c3ea96a29d954833b639ec23376b0a1e'  # Replace with your NewsAPI key
NEWSAPI_URL = "https://newsapi.org/v2/everything"

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

# Function to get daily news from NewsAPI for a given stock and date
def get_news_for_stock(stock_symbol, date):
    params = {
        'q': stock_symbol,  # Query the stock symbol
        'from': date.strftime('%Y-%m-%d'),
        'to': date.strftime('%Y-%m-%d'),
        'sortBy': 'relevancy',  # Sort by relevancy
        'language': 'en',  # Filter news in English
        'apiKey': NEWS_API_KEY,
        'pageSize': 5  # Limit to 5 articles
    }
    
    response = requests.get(NEWSAPI_URL, params=params)
    
    if response.status_code == 200:
        data = response.json().get('articles', [])
        return [article['description'] for article in data]
    else:
        print(f"Error fetching news for {stock_symbol} on {date}")
        return []

# Function to preprocess article (dummy placeholder, you can add custom steps)
def preprocess_article(article):
    return article.lower().replace('\n', ' ').strip()

# Function to perform sentiment analysis on an article
def analyze_sentiment(article):
    result = finbert(article)[0]
    label = result['label']
    if label == 'Positive':
        return 1
    elif label == 'Negative':
        return -1
    else:
        return 0

# Function to compute the overall sentiment score for a given day's news
def compute_overall_sentiment(sentiment_scores):
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
    if avg_sentiment > 0.33:
        return 1
    elif avg_sentiment < -0.33:
        return -1
    else:
        return 0
    
def fetch_sentiment_data(stocks, start_date, end_date):

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    current_date = start_date
    # Create an empty DataFrame to store the results
    results = pd.DataFrame(columns=['Date', 'Stock', 'OverallSentimentScore'])

    # Main loop to collect data for each stock, each day, and perform sentiment analysis
    
    while current_date < end_date:
        for stock_symbol in stocks:
            news_articles = get_news_for_stock(stock_symbol, current_date)
            
            if not news_articles:
                continue

            # Preprocess and analyze sentiment for each article
            sentiment_scores = []
            for article in news_articles:
                # if article is None then replace it to empty string
                if article is None:
                    article = ""
                cleaned_article = preprocess_article(article)
                sentiment_score = analyze_sentiment(cleaned_article)
                sentiment_scores.append(sentiment_score)
            
            # Compute the overall sentiment score for the day
            overall_sentiment = compute_overall_sentiment(sentiment_scores)
            
            # Append the result to the DataFrame
            results = results.append({
                'Date': current_date.strftime('%Y-%m-%d'),
                'Stock': stock_symbol,
                'OverallSentimentScore': overall_sentiment
            }, ignore_index=True)
        
        # Move to the next day
        current_date += timedelta(days=1)

    # Concatenate all stock data into a single DataFrame
    final_df = pd.DataFrame(results)
    
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
    sentiment_df = fetch_sentiment_data(stock_symbols, start_date, end_date)
    
    # Show the first few rows of the dataframe
    print(sentiment_df.head())
    
    # Optionally save the dataframe to a CSV file
    sentiment_df.to_csv('sentiment_data.csv', index=False)