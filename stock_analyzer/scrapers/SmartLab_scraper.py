#Файл для парсинга всей информации со SmartLab
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import time


def extract_indicator(table, field):
    row = table.find_element(By.XPATH, f".//tr[@field='{field}']")
    return row.find_element(By.TAG_NAME, "th").find_element(By.TAG_NAME, "a").text

def extract_yaers(table):
    row = table.find_element(By.XPATH, f".//tr[@class='header_row']")
    cells = row.find_elements(By.TAG_NAME, "td")
    values = [np.nan] * 5
    for i in range(5):
        if i + 1 < len(cells):
            text = cells[i+1].text.strip().replace(' ', '')
            values[i] = int(text) if text else np.nan
    return values

def extract_init(table, field):
    row = table.find_element(By.XPATH, f".//tr[@field='{field}']")
    return row.find_element(By.TAG_NAME, "th").find_element(By.TAG_NAME, "span").text


def get_info(driver, ticker, time, em_id):
    time_d = {
        "Период" : time
    }

    url = f"https://smart-lab.ru/q/{ticker}/f/{time}/"
    driver.get(url)
    table = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'simple-little-table') and contains(@class, 'financials')]")))
    years = extract_yaers(table)
    all_field = table.find_elements(By.XPATH, f".//tr[@field]")
    for i in range(len(years)):
        value_d = {}
        for j in all_field[5::]:
            name = j.find_elements(By.TAG_NAME, "th")[0].text
            if name == "IR рейтинг":
                break
            else:
                value = j.find_elements(By.TAG_NAME, "td")
                if value[i+1].text == "" or value[i+1].text == " ":
                    value_d[name] = None
                else:
                    value_d[name] = value[i+1].text
                    

        time_d[str(years[i])] = value_d
    return {em_id : time_d}  #Вся инфа из таблицы

    

            
def get_sector_name(driver, ticker):
    url = f"https://smart-lab.ru/q/{ticker}/f/y/"
    driver.get(url)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    sector_name = []
    page_source = driver.page_source

    sector_elements = soup.find_all('a', 
                                class_='js-tooltip-target', 
                                attrs={'data-title'})

    for element in sector_elements:
        sector_title = element['data-title']
        if "Aнализ сектора " in sector_title:
            sector = sector_title.split()
            for i in range (len(sector) - 1, 0, -1):
                if sector[i] == "сектора":
                    break
                sector_name.append(sector[i])
            break
    return " ".join(sector_name)
