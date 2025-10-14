import pandas as pd
import numpy as np
from config import TICKERS, APITOKEN
from parsers.moex_parser import get_moex_data, get_security_info, get_inn_and_okpo, get_last_price
from data.processor import format_number, format_from_db
from db.MongoDB_handler import insert_compains, update_compains_info, inser_info, find_info
from scrapers.SmartLab_scraper import get_sector_name, get_info
from scrapers.Tinkoff_scraper import start_main
from visualization.main_vis import start
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time


chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=Service("C:\\Users\\Cronn\\stock_analyzer\\chromedriver.exe"), options=chrome_options)
start()
driver.quit
