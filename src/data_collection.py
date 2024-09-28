import yfinance as yf
import pandas as pd
import yaml
import os
from datetime import timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Function to load YAML configuration
def load_yaml_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Function to fetch stock data from Yahoo Finance
def fetch_stock_data(stocks, start_date, end_date):
    all_data = []
    for stock in stocks:
        stock_data = yf.download(stock, start=start_date, end=end_date)
        stock_data['Stock'] = stock
        stock_data.reset_index(inplace=True)
        stock_data = stock_data[['Date', 'Stock', 'Open', 'High', 'Low', 'Close', 'Volume']]
        all_data.append(stock_data)
    final_df = pd.concat(all_data, ignore_index=True)
    return final_df

# Function to preprocess article (placeholder)
def preprocess_article(article):
    return article

# Function to analyze sentiment using VADER
def analyze_sentiment(article):
    analyzer = SentimentIntensityAnalyzer()
    sentiment_score = analyzer.polarity_scores(article)['compound']
    if sentiment_score > 0.05:
        return 1
    elif sentiment_score < -0.05:
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

# Main function to perform stock data collection and sentiment analysis
def main():
    # Load configurations
    conf_folder = os.path.join(os.getcwd(), 'conf')
    general_config_path = os.path.join(conf_folder, 'general_config.yml')
    instrument_list_path = os.path.join(conf_folder, 'instrument_list.yml')

    general_config = load_yaml_config(general_config_path)
    instrument_list = load_yaml_config(instrument_list_path)

    start_date = general_config['start_date']
    end_date = general_config['end_date']
    stock_symbols = instrument_list['stocks']

    # Fetch stock data
    stock_df = fetch_stock_data(stock_symbols, start_date, end_date)

    # Create an empty DataFrame to store sentiment analysis results
    sentiment_results = pd.DataFrame(columns=['Date', 'Stock', 'Overall Sentiment Score'])

    # Perform sentiment analysis for each stock and day
    current_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while current_date <= end_date:
        for stock_symbol in stock_symbols:
            news_articles = get_news_for_stock(stock_symbol, current_date)  # Placeholder function

            if not news_articles:
                continue

            sentiment_scores = []
            for article in news_articles:
                cleaned_article = preprocess_article(article if article else "")
                sentiment_score = analyze_sentiment(cleaned_article)
                sentiment_scores.append(sentiment_score)

            overall_sentiment = compute_overall_sentiment(sentiment_scores)
            sentiment_results = sentiment_results.append({
                'Date': current_date.strftime('%Y-%m-%d'),
                'Stock': stock_symbol,
                'Overall Sentiment Score': overall_sentiment
            }, ignore_index=True)

        current_date += timedelta(days=1)

    # Merge sentiment analysis with stock data
    stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.strftime('%Y-%m-%d')
    merged_df = pd.merge(stock_df, sentiment_results, on=['Date', 'Stock'], how='left')

    # Save final merged DataFrame to CSV
    merged_df.to_csv('merged_stock_sentiment_data.csv', index=False)

    print("Stock data and sentiment analysis successfully merged and saved to 'merged_stock_sentiment_data.csv'.")

# Placeholder for news fetching function (to be implemented)
def get_news_for_stock(stock_symbol, current_date):
    return []  # Placeholder: Replace with actual implementation to fetch news

if __name__ == "__main__":
    main()
