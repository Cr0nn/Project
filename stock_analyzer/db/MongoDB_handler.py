#Файл для работы с MongoDB
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from numpy import average, median
import numpy as np
from config import MONGODB_URI
from config import DB_NAME

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

def insert_compains(data, ticker):
    companies_collection = db['Companies']
    company_data = {
        "_id" : data["EMITTER_ID"],
        "name" : data["NAME"],
        "sector" : data["SECTOR"],
        "inn" : data["inn"],
        "okpo" : data["okpo"],
        "ticker" : ticker
    }
    result = companies_collection.insert_one(company_data)

def inser_info(data):
    info_collection = db["Finans_info"]
    info_collection.insert_one(data)



def update_compains_info(data, id):
    companies_collection = db['Companies']
    for key, value in data.items():
        companies_collection.update_one(
            {"id" : id},
            {"$set": {key : value}}
        )

def get_all_tickers():
    companies_collection = db['Companies']
    docs = companies_collection.find({}, {'ticker' : 1})
    return [i['ticker'] for i in docs]

def find_info(em_id):
    collections = db["Finans_info"]
    docs = collections.find({
        em_id : {'$exists':True},
        em_id : {'$type': 'object'}
    })
    return docs 

def get_all_em_id():
    collections = db["Companies"]
    docs = collections.find({}, {'_id': 1})
    result = [i["_id"] for i in docs]
    return result #Все id Компаний

# def find_all_sector():
#     collections = db[""]
#     docs = collections.find_one({"_id":"Sectors"})
#     all_sector = [key for key, value in docs.items() if key != "_id"]
#     all_sector.append("Все секторы")
#     return all_sector

def get_em_name(em_id):
    collections = db["Companies"]
    result = [collections.find_one({'_id': i}, {'name' : 1})["name"] for i in em_id]
    return result #Все имена компнаий 

def find_id_by_name(name):
    colletions = db["Companies"]
    return colletions.find_one(({'name' : name}))["_id"] #Имя компании

def get_companies_in_sector(sector):
    colletions = db["Companies"]
    result = []
    docs = colletions.find({})
    if sector == "Все секторы":
        result =  [i["name"] for i in docs]
    else: 
        result = [i["name"] for i in docs if i["sector"] == sector]
    return result


def get_base_info(em_id):
    collections = db["Companies"]
    return collections.find_one({'_id':em_id}) #Весь документ Compains по id

def get_none_value(TICKERS): #Тест функция, пока будет
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    all_none = []
    for i in TICKERS:
        id = collections1.find_one({"ticker": i})["_id"]
        if collections2.find_one({id:"Нет данных"}) != None:
            all_none.append(collections1.find_one({"_id":id})["ticker"])
    return all_none


def PE_filter(Companies):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    none_check = lambda x: "0" if x is None else x.replace(" ", "") #Функция отлавливания значений None в таблице + замена пробелов в числах формата 1 689
    PE = {}
    avg = []
    for i in Companies:
        Current_PE = []
        id = collections1.find_one({"name":i})["_id"]
        Compain = collections2.find_one({id:{'$exists':True}})[id]
        try:
            for j in Compain:
                if j != "Период":
                    Current_PE.append(float(none_check(Compain[j]["P/E"])))
            PE[i] = ([round(average(Current_PE),2), round(Current_PE[-1],2)])
            avg.append(round(average(Current_PE),2))
        except Exception as e:
            print(f"Ошибка в компании {j} вида {e}")
            continue
    return PE, round(average(avg),2)

def div_filter(Companies):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    Filtred = []
    for i in Companies:
        div_yield = []
        id = collections1.find_one({"name":i})["_id"]
        Compain = collections2.find_one({id:{'$exists': True}})[id]
        try:
            for j in Compain:
                if j != "Период":
                    if Compain[j]["Див доход, ао, %"] != None:
                        div_yield.append(Compain[j]["Див доход, ао, %"])
            if ("0%" not in div_yield) and ("0.0%" not in div_yield):
                Filtred.append(i)
        except:
            continue
    return Filtred

def debt_filter(Companies):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    none_check = lambda x: "0" if x is None else x.replace(" ", "") #Функция отлавливания значений None в таблице + замена пробелов в числах формата 1 689
    debt_dict = {}
    for i in Companies:
        id = collections1.find_one({"name":i})["_id"]
        sector = init_sector(i)
        Company = collections2.find_one({id:{'$exists':True}})[id]
        try:
            if sector != "Банки":
                debt = np.array([float(none_check(Company[j]["Долг, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                net_debt = np.array([float(none_check(Company[j]["Чистый долг, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                ebitda = np.array([float(none_check(Company[j]["EBITDA, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                equity = np.array([float(none_check(Company[j]["Чистые активы, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                capex = np.array([float(none_check(Company[j]["CAPEX, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                cash = np.array([float(none_check(Company[j]["Наличность, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                fcf = np.array([float(none_check(Company[j]["FCF, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                cap = np.array([float(none_check(Company[j]["Капитализация, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                perc_exp = np.array([float(none_check(Company[j]["Процентные расходы, млрд руб"])) for j in Company if j !="Период" and sector != "Финансовый сектор"])
                capex_to_rev = np.array([float(none_check(Company[j]["CAPEX/Выручка, %"]).replace("%", "")) for j in Company if j!="Период" and sector != "Финансовый сектор"])
                capex_to_rev = capex_to_rev/100
                # Безопасное деление для revenue
                revenue = np.divide(capex, capex_to_rev, 
                                    out=np.zeros_like(capex, dtype=float), 
                                    where=(capex_to_rev!=0) & ~np.isnan(capex_to_rev))
                
                # А теперь сами метрики
                metrics = {
                    "Debt/Equity": np.divide(debt, equity, out=np.zeros_like(debt, dtype=float), where=(equity!=0) & ~np.isnan(equity)),
                    "NetDebt/EBITDA": np.divide(net_debt, ebitda, out=np.zeros_like(net_debt, dtype=float), where=(ebitda!=0) & ~np.isnan(ebitda)),
                    "NetDebt/Revenue": np.divide(net_debt, revenue, out=np.zeros_like(net_debt, dtype=float), where=(revenue!=0) & ~np.isnan(revenue)),
                    "Cash/Debt": np.divide(cash, debt, out=np.zeros_like(cash, dtype=float), where=(debt!=0) & ~np.isnan(debt)),
                    "EBITDA/Interest": np.divide(ebitda, perc_exp, out=np.zeros_like(ebitda, dtype=float), where=(perc_exp!=0) & ~np.isnan(perc_exp)),
                }

                # Очистим от остатков NaN (если они всё же где-то просочились)
                metrics = {k: np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0) for k, v in metrics.items()}
                metrics = {k: np.round(v, 2) for k, v in metrics.items()}
                debt_dict[i] = metrics
            else:
                credit = np.array([float(none_check(Company[j]["Кредитный портфель, млрд руб"])) 
                    for j in Company if j != "Период"])
                deposits = np.array([float(none_check(Company[j]["Депозиты, млрд руб"])) 
                                    for j in Company if j != "Период"])
                assets = np.array([float(none_check(Company[j]["Активы банка, млрд руб"])) 
                                for j in Company if j != "Период"])
                equity = np.array([float(none_check(Company[j]["Капитал, млрд руб"])) 
                                for j in Company if j != "Период"])
                profit = np.array([float(none_check(Company[j]["Чистая прибыль, млрд руб"])) 
                                for j in Company if j != "Период"])

                # --- Метрики ---
                metrics = {
                    "Loan/Deposit": np.divide(credit, deposits, out=np.zeros_like(credit), where=(deposits != 0) & ~np.isnan(deposits)),
                    "Equity/Assets": np.divide(equity, assets, out=np.zeros_like(equity), where=(assets != 0) & ~np.isnan(assets)),
                    "Loans/Assets": np.divide(credit, assets, out=np.zeros_like(credit), where=(assets != 0) & ~np.isnan(assets)),
                    "Deposits/Assets": np.divide(deposits, assets, out=np.zeros_like(deposits), where=(assets != 0) & ~np.isnan(assets)),
                    "Loans/Capital": np.divide(credit, equity, out=np.zeros_like(credit), where=(equity != 0) & ~np.isnan(equity)),
                    "ROA": np.divide(profit, assets, out=np.zeros_like(profit), where=(assets != 0) & ~np.isnan(assets)),
                    "ROE": np.divide(profit, equity, out=np.zeros_like(profit), where=(equity != 0) & ~np.isnan(equity)),
                }

                # Очистка NaN, ±∞
                metrics = {k: np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0) for k, v in metrics.items()}
                metrics = {k: np.round(v, 3) for k, v in metrics.items()}

                debt_dict[i] = metrics
        except Exception as e:
            print(e)
    return debt_dict

def ROE_filter(Companies):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    none_check = lambda x: "0" if x is None or x == "null" else x.replace(" ", "").replace("%", "") #Функция отлавливания значений None в таблице + замена пробелов в числах формата 1 689
    ROE = {}
    avg = []
    for i in Companies:
        Current_ROE = []
        id = collections1.find_one({"name":i})["_id"]
        Compain = collections2.find_one({id:{'$exists':True}})[id]
        for j in Compain:
            data = Compain[j]
            if j != "Период":
                try:
                    Current_ROE.append(float(none_check(data["ROE, %"])))
                except:
                    try: 
                        Current_ROE.append(float(none_check(data["Рентабельность банка, %"]))) 
                    except:
                        try:
                            profit = float(none_check(data.get("Чистая прибыль, млрд руб", "0")))
                            equity = float(none_check(data.get("Чистые активы, млрд руб", data.get("Капитал, млрд руб", None))))
                            if equity == 0.0: equity = round(float(none_check(data.get("Активы, млрд руб"))) - float(none_check(data.get("Долг, млрд руб"))),2)
                            val = (profit / equity * 100) if equity != 0.0 else 0.0
                            Current_ROE.append(val)
                        except Exception as e:
                            print(e)
                            continue
        print(Current_ROE, i)
        ROE[i] = ([round(median(Current_ROE),2), round(Current_ROE[-1],2)])
        avg.append(round(median(Current_ROE),2))
    return ROE, median(avg)

def init_sector(Company):
    collections1 = db["Companies"]
    return collections1.find_one({'name':Company})['sector']



