import requests
import numpy as np

def get_moex_data(ticker):
    """
    Получает текущую цену и другую информацию по тикеру с Московской биржи.
    """
    # Формируем URL запроса для акций основного режима торгов (TQBR)
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json'
    
    # Делаем запрос к API
    response = requests.get(url)
    data = response.json()
    
    # Данные о бумаге находятся в секции 'securities'
    # Данные о текущей цене (последней сделке) - в секции 'marketdata'
    securities_info = data['securities']['data']
    marketdata_info = data['marketdata']['data']
    
    if securities_info and marketdata_info:
        security_data = securities_info[0]
        market_data = marketdata_info[0]
    
        return market_data[12]
    
def get_security_info(ticker: str):

    url = f"https://iss.moex.com/iss/securities/{ticker}.json"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    sec_table = data.get("description")
    info = sec_table.get("data")

    data_info = {info[0][0] : info[0][2]}
    for i in range(1, len(info)):
        data_info[info[i][0]] = info[i][2]
    return data_info # {'SECID', 'ISSUENAME', 'NAME', 'SHORTNAME', 'ISIN', 'REGNUMBER', 'ISSUESIZE', 'FACEVALUE', 'FACEUNIT', 'ISSUEDATE', 'LATNAME', 'HASPROSPECTUS', 'DECISIONDATE', 'HASDEFAULT', 'HASTECHNICALDEFAULT', 
                #'EMITENTMISMATCHCUR', 'LISTLEVEL', 'ISQUALIFIEDINVESTORS', 'MORNINGSESSION', 'EVENINGSESSION', 'WEEKENDSESSION', 'REGISTRY_DATE', 'TYPENAME', 'GROUP', 'TYPE', 'GROUPNAME', 'EMITTER_ID'}

def get_inn_and_okpo(id: str):

    url = f"https://iss.moex.com/iss/emitters/{id}.json"

    response = requests.get(url)
    data = response.json()

    table = data.get("emitter")
    info = table.get("data")

    return  {
        "inn" : info[0][3],
        "okpo" : info[0][5]
    }

def get_last_price(tickers: str):

    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?iss.only=marketdata"

    data_info = [] 
    response = requests.get(url)
    response.raise_for_status()
    marketdata = response.json()

    table = marketdata.get("marketdata")
    col = table.get("columns")
    data = table.get("data")

    for i in range(len(data)):
        if data[i][0] in tickers:          
            info = {
                "ticker" : data[i][col.index("SECID")],
                "open_price": data[i][col.index("OPEN")],
                "last_price": data[i][col.index("LAST")],
                "high_price": data[i][col.index("HIGH")],
                "low_price": data[i][col.index("LOW")]
            }
            data_info.append(info)
            if len(data_info) == len(tickers):
                break
    return data_info




        
    
    
    
    

