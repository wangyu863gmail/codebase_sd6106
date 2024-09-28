import pandas as pd
import numpy as np
from nltk.corpus import stopwords
import re
from transformers import pipeline
import yaml

class DataProcessor:
    def __init__(self, config_path):
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        
        self.sentiment_model = config['sentiment_model']
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=self.sentiment_model)

    # ... rest of the class implementation remains the same ...

# Usage example
# processor = DataProcessor('../conf/config.yml')
# cleaned_financial_data = processor.process_financial_data(financial_data)
# cleaned_text = processor.clean_text(raw_text)
# sentiment = processor.analyze_sentiment(cleaned_text)
# aggregated_sentiment = processor.aggregate_sentiment(list_of_sentiments)