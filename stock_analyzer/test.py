from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from config import APITOKEN
from db.MongoDB_handler import get_all_em_id, get_em_name, find_id_by_name, get_base_info
import numpy as np
import time



'''url = f"https://smart-lab.ru/q/GAZP/f/y/"

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=Service("C:\\Users\\Cronn\\stock_analyzer\\chromedriver.exe"), options=chrome_options)

driver.get(url)
time.sleep(2)
table = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'simple-little-table') and contains(@class, 'financials')]")))
row = table.find_element(By.XPATH, f".//tr[@field='headrer_row']")
indicator  = row.find_element(By.TAG_NAME, "th").find_element(By.TAG_NAME, "a").text
unit = row.find_element(By.TAG_NAME, "th").find_element(By.TAG_NAME, "span").text
cells = row.find_elements(By.TAG_NAME, "td")
values = [np.nan] * 5
for i in range(5):
    if i + 1 < len(cells):
        text = cells[i + 1].text.strip().replace(' ', '').replace(',', '.').replace('%', '')
        values[i] = float(text) if text and text.replace('.', '').replace('-', '').isdigit() else np.nan

print(indicator, unit)'''
