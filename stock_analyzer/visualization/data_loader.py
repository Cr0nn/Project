import pandas as pd
import numpy as np
from datetime import datetime
import requests

def get_sample_data(data):
    return pd.DataFrame(data)

def get_moex_data(ticker):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json"
    response = requests.get(url)
    data = response.json()
    if data['securities']['data']:
        quotes = data['securities']['data'][0]
        return float(quotes[3]), float(quotes[4])  # Цена, объём
    return np.nan, np.nan