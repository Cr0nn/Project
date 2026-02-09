#Файл для работы с MongoDB
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo import InsertOne
from pymongo.operations import UpdateOne
from pymongo.collection import Collection
from pymongo.operations import ReplaceOne
from numpy import average, median
import numpy as np
from config_folder.config import MONGODB_URI
from config_folder.config import DB_NAME
from datetime import datetime, timedelta
from collections import defaultdict



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
    pipeline = [
        {
            '$project' : 
                {
                    '_id':0,
                    'ticker':'$ticker'
                }
        }
    ]
    collection = list(companies_collection.aggregate(pipeline))
    return [i['ticker'] for i in collection]

def get_all_name():
    companies_collection = db["Companies"]
    pipeline = [
        {
            "$project" : 
                {
                    "_id" : 0,
                    "name" : "$name"
                }
        }
    ]
    collection = list(companies_collection.aggregate(pipeline))
    return [i["name"] for i in collection]

def find_info(em_id):
    collections = db["Finans_info"]
    docs = collections.find({
        "id" : em_id
    })
    return docs 

def get_all_em_id():
    collections = db["Companies"]
    docs = collections.find({}, {'_id': 1})
    result = [i["_id"] for i in docs]
    return result #Все id Компаний


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

def get_last_hour_price(name):
    collection = db["prices_1m"]
    ticker = list(db["Companies"].find({"name" : name}, {"ticker" : 1, "_id" : 0}))[0]["ticker"]

    pipeline = [
        {"$match": {"meta.ticker": ticker}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$timestamp",
                        "unit": "minute"
                    }
                },
                "price": {"$last": "$price"}
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 60},
        {"$sort": {"_id": 1}}
    ]
    
    collection = list(collection.aggregate(pipeline))
    delete_zero()
    return collection

def get_last_hour_price_5(name):
    collection = db["prices_5m"]
    ticker = list(db["Companies"].find({"name" : name}, {"ticker" : 1, "_id" : 0}))[0]["ticker"]

    pipeline = [
        {"$match": {"meta.ticker": ticker}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$timestamp",
                        "unit": "minute"
                    }
                },
                "price": {"$last": "$close"}
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 60},
        {"$sort": {"_id": 1}}
    ]
    
    collection = list(collection.aggregate(pipeline))
    delete_zero()
    return collection


def PE_filter(Companies, sector):
    collections2 = db["Finans_info"]
    PE = {}
    for i in Companies:
        Current_PE = []
        pipeline = [
            {
                "$lookup": {
                    "from": "Companies",
                    "localField": "id", 
                    "foreignField": "_id",
                    "as": "Company"
                }
            },
            {
                "$unwind" : "$Company"
            },
            {
                "$match" : {"Company.name" : i}
            },
            {
                "$project" : 
                    {
                        "document_id" : "$_id",
                        "info_array" : {"$objectToArray" : "$info"}
                    }
            },
            {
                "$project" :
                    {
                        "document_id": 1,
                        "pe_metrics":
                            {
                                "$map" :
                                    {
                                        "input" : 
                                            {
                                                "$filter" : 
                                                    {
                                                        "input" : "$info_array",
                                                        "as" : "item",
                                                        "cond" : {"$ne" : ["$$item.k", "Период"]}
                                                    }
                                            },
                                        "as" : "year_data",
                                        "in" : 
                                            {
                                                "k": "$$year_data.k",
                                                "v": 
                                                    {"$convert":
                                                        {
                                                            "input" : "$$year_data.v.P/E",
                                                            "to" : "double",
                                                            "onError" : 0.0,
                                                            "onNull" : 0.0
                                                        }
                                                    }
                                            }
                                    }
                            }
                    }
            },
            {
                "$project" : {"_id": 0, "PE" : '$pe_metrics'}
            }
        ]

        collection = list(collections2.aggregate(pipeline))[0]['PE']
        filtred_pe = np.array([i['v'] for i in collection], dtype=np.float64)
        if len(filtred_pe) >= 3:
            PE[i] = filtred_pe[~np.isnan(filtred_pe)]
    return PE, avg_metric(sector, "P/E")

def ROE_filter(Companies, sector):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    none_check = lambda x: "0" if x is None or x == "null" else x.replace(" ", "").replace("%", "") #Функция отлавливания значений None в таблице + замена пробелов в числах формата 1 689
    ROE = {}
    avg = []
    for i in Companies:
        pipeline = [
            {
            "$lookup" : 
                {
                    "from" : "Companies",
                    "localField" : "id",
                    "foreignField" : "_id",
                    "as" : "Company"
                }
            },
            {
                "$unwind" : "$Company"
            },
            {
                "$match" : {"Company.name":i}
            },
            {
                "$project" : 
                    {
                        "document_id" : "$_id",
                        "info_array" : {"$objectToArray" : "$info"}
                    }
            },
            {
                "$project" : 
                    {
                        "document_id" : 1,
                        "ROE_metric" : {
                            "$map" : 
                                {
                                    "input" : 
                                    {
                                        "$filter": 
                                            {
                                                "input" : "$info_array",
                                                "as" : "item",
                                                "cond" : {"$ne" : ["$$item.k", "Период"]}
                                            }
                                    },
                                    "as" : "year_data",
                                    "in" : 
                                        {
                                            "k": "$$year_data.k",
                                            "v": 
                                            {
                                                "ROE": 
                                                    {"$toDouble" :
                                                        {
                                                                "$replaceAll" : 
                                                                    {
                                                                        "input" : 
                                                                            {
                                                                                "$replaceAll" : 
                                                                                    {
                                                                                        "input" : "$$year_data.v.ROE, %",
                                                                                        "find" : " ",
                                                                                        "replacement" : ''
                                                                                    }
                                                                            },
                                                                        "find" : "%",
                                                                        "replacement" : ""   
                                                                    }
                                                        }
                                                    },
                                                "ROE_alt":
                                                    {
                                                        "$toDouble": 
                                                            {
                                                                    "$replaceOne" : 
                                                                        {
                                                                            "input" : {
                                                                                "$replaceAll" : 
                                                                                    {
                                                                                        "input" : "$$year_data.v.Рентабельность банка, %",
                                                                                        "find" : " ",
                                                                                        "replacement" : ""
                                                                                    }
                                                                                },
                                                                            "find" : "%",
                                                                            "replacement" : ""
                                                                        }
                                                            }
                                                    },
                                                "profit" : 
                                                    {
                                                        "$convert":
                                                        {
                                                            "input" : "$$year_data.v.Чистая прибыль, млрд руб",
                                                            "to" : "double",
                                                            "onError" : None,
                                                            "onNull" : None
                                                        }
                                                    },
                                                "equity" : 
                                                    {
                                                        "$convert" : 
                                                            {
                                                                "input" : "$$year_data.v.Чистые активы, млрд руб",
                                                                "to" : "double",
                                                                "onError" : None,
                                                                "onNull" : None
                                                            }
                                                    }                                      
                                            }

                                        }
                                },
                                
                        }
                    }
            },
            {
                "$project": {
                    "_id": 0,
                    "ROE_dict": {
                        "$map": {
                            "input": "$ROE_metric",
                            "as": "year_item",
                            "in": {
                                "k": "$$year_item.k",
                                "v": {
                                    "$arrayToObject": {
                                        "$filter": {
                                            "input": {"$objectToArray": "$$year_item.v"},
                                            "as": "field",
                                            "cond": {"$ne": ["$$field.v", None]}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
        collection = list(collections2.aggregate(pipeline))[0]["ROE_dict"]
        keys = list(collection[0]["v"].keys())
        if 'ROE' in keys:
            arr = np.array([i["v"].get("ROE") for i in collection], dtype=np.float64)
            if len(arr) >= 3:
                ROE[i] = arr[~np.isnan(arr)]
        elif "ROE_alt" in keys:
            arr = np.array([i["v"].get("ROE_alt") for i in collection], dtype=np.float64)
            if len(arr) >= 3:
                ROE[i] = arr[~np.isnan(arr)]
        elif "profit" in keys and "equity" in keys:
            arr = (np.array([i["v"].get("profit") for i in collection], dtype=np.float64) / np.array([i["v"].get("equity") for i in collection], dtype=np.float64)) * 100
            if len(arr) >= 3:
                ROE[i] = arr[~np.isnan(arr)]
    return ROE, avg_metric(sector, "ROE")


def debt_filter(Companies, sector):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    DEBT = {}
    for i in Companies:
        pipeline = [
            {
                "$lookup" : 
                    {
                        "from" : "Companies",
                        "localField" : "id",
                        "foreignField" : "_id",
                        "as" : "Company"
                    }
            },
            {
                "$unwind" : "$Company"
            },
            {
                "$match" : {"Company.name" : i}
            },
            {
                "$project" : 
                    {
                        "_id" : 0,
                        "info_array" : {"$objectToArray" : "$info"}
                    }
            },
            build_debtmetric_project(sector),
            {
                "$project": 
                    {
                        "_id" : 0,
                        "Debt_dict" : 
                            {
                                "$map" : 
                                    {
                                        "input" : "$Debt_metric",
                                        "as" : "year_item",
                                        "in" : 
                                            {
                                                "k" : "$$year_item.k",
                                                "v" : 
                                                    {
                                                        "$arrayToObject" : 
                                                            {
                                                                "$filter" : 
                                                                    {
                                                                        "input" : {
                                                                            "$map" : 
                                                                            {
                                                                                "input" : {"$objectToArray" : "$$year_item.v"},
                                                                                "as" : "field",
                                                                                "in" : {
                                                                                    "k" : "$$field.k",
                                                                                    "v" : {
                                                                                        "$toDouble" : 
                                                                                            {
                                                                                                "$replaceAll" : 
                                                                                                    {
                                                                                                        "input" : 
                                                                                                            {
                                                                                                                "$replaceAll" : 
                                                                                                                    {
                                                                                                                        "input" : "$$field.v",
                                                                                                                        "find" : "%",
                                                                                                                        "replacement" : " "
                                                                                                                    }
                                                                                                            },
                                                                                                        "find" : " ",
                                                                                                        "replacement" : ""
                                                                                                    }
                                                                                            }
                                                                                    }
                                                                                }
                                                                            }
                                                                        },
                                                                        "as" : "field",
                                                                        "cond" : {"$ne" : ["$$field.v", None]}
                                                                    }
                                                            }
                                                    }
                                            }
                                    }
                            }
                    }
            }
        ]
        collection = list(collections2.aggregate(pipeline))[0]["Debt_dict"]
        if sector != "Банки":
            debt = np.array([k["v"].get('debt') for k in collection], dtype=np.float64)
            equity = np.array([k["v"].get('equity') for k in collection], dtype=np.float64)
            net_debt = np.array([k["v"].get('net_debt') for k in collection], dtype=np.float64)
            ebitda = np.array([k["v"].get('ebitda') for k in collection], dtype=np.float64)
            capex = np.array([k["v"].get('capex') for k in collection], dtype=np.float64)
            cash = np.array([k["v"].get('cash') for k in collection], dtype=np.float64)
            fcf = np.array([k["v"].get('fcf') for k in collection], dtype=np.float64)
            cap = np.array([k["v"].get('cap') for k in collection], dtype=np.float64)
            perc_exp = np.array([k["v"].get('perc_exp') for k in collection], dtype=np.float64)
            capex_to_rev = (np.array([k["v"].get('capex_to_rev') for k in collection], dtype=np.float64))/100
            revenue = np.divide(capex, capex_to_rev, 
                                out=np.zeros_like(capex, dtype=float), 
                                where=(capex_to_rev!=0) & ~np.isnan(capex_to_rev))
            metrics = {
                "Debt/Equity": np.divide(debt, equity, out=np.zeros_like(debt, dtype=float), where=(equity!=0) & ~np.isnan(equity)),
                "NetDebt/EBITDA": np.divide(net_debt, ebitda, out=np.zeros_like(net_debt, dtype=float), where=(ebitda!=0) & ~np.isnan(ebitda)),
                "NetDebt/Revenue": np.divide(net_debt, revenue, out=np.zeros_like(net_debt, dtype=float), where=(revenue!=0) & ~np.isnan(revenue)),
                "Cash/Debt": np.divide(cash, debt, out=np.zeros_like(cash, dtype=float), where=(debt!=0) & ~np.isnan(debt)),
                "EBITDA/Interest": np.divide(ebitda, perc_exp, out=np.zeros_like(ebitda, dtype=float), where=(perc_exp!=0) & ~np.isnan(perc_exp)),
            }
        else:
            credit = np.array([k["v"].get('credit') for k in collection], dtype=np.float64)
            deposits = np.array([k["v"].get('deposits') for k in collection], dtype=np.float64)
            assets = np.array([k["v"].get('assets') for k in collection], dtype=np.float64)
            equity = np.array([k["v"].get('equity') for k in collection], dtype=np.float64)
            profit = np.array([k["v"].get('profit') for k in collection], dtype=np.float64)
            metrics = {
                "Loan/Deposit": np.divide(credit, deposits, out=np.zeros_like(credit), where=(deposits != 0) & ~np.isnan(deposits)),
                "Equity/Assets": np.divide(equity, assets, out=np.zeros_like(equity), where=(assets != 0) & ~np.isnan(assets)),
                "Loans/Assets": np.divide(credit, assets, out=np.zeros_like(credit), where=(assets != 0) & ~np.isnan(assets)),
                "Deposits/Assets": np.divide(deposits, assets, out=np.zeros_like(deposits), where=(assets != 0) & ~np.isnan(assets)),
                "Loans/Capital": np.divide(credit, equity, out=np.zeros_like(credit), where=(equity != 0) & ~np.isnan(equity)),
                "ROA": np.divide(profit, assets, out=np.zeros_like(profit), where=(assets != 0) & ~np.isnan(assets)),
                "ROE": np.divide(profit, equity, out=np.zeros_like(profit), where=(equity != 0) & ~np.isnan(equity)),
            }
        metrics = {k: np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0) for k, v in metrics.items()}
        metrics = {k: np.round(v, 2) for k, v in metrics.items()} 
        DEBT[i] = metrics
    return DEBT

def build_debtmetric_project(sector):
    if sector != "Банки":
        return {
            "$project" : 
                {
                    "_id" : 0,
                    "Debt_metric" :
                        {
                            "$map":
                            {
                                "input" : 
                                    {
                                        "$filter" : 
                                            {
                                                "input" : "$info_array",
                                                "as" : "item",
                                                "cond" : {"$ne" : ["$$item.k", "Период"]}
                                            }
                                    },
                                    "as" : "year_data",
                                    "in" : 
                                        {
                                            "k" : "$$year_data.k",
                                            "v" : 
                                                {
                                                    "debt" : "$$year_data.v.Долг, млрд руб",
                                                    "net_debt" : "$$year_data.v.Чистый долг, млрд руб",
                                                    "ebitda" : "$$year_data.v.EBITDA, млрд руб",
                                                    "equity" : "$$year_data.v.Чистые активы, млрд руб",
                                                    "capex" : "$$year_data.v.CAPEX, млрд руб",
                                                    "capex_to_rev" : "$$year_data.v.CAPEX/Выручка, %",
                                                    "cash" : "$$year_data.v.Наличность, млрд руб",
                                                    "fcf" : "$$year_data.v.FCF, млрд руб",
                                                    "cap" : "$$year_data.v.Капитализация, млрд руб",
                                                    "perc_exp" : "$$year_data.v.Процентные расходы, млрд руб"

                                                }
                                        }
                            }
                        }
                }
            }
    else:
        return {
            "$project" : 
                {
                    "_id" : 0,
                    "Debt_metric" :
                        {
                            "$map":
                            {
                                "input" : 
                                    {   
                                        "$filter" : 
                                            {
                                                "input" : "$info_array",
                                                "as" : "item",
                                                "cond" : {"$ne" : ["$$item.k", "Период"]}
                                            }
                                    },
                                    "as" : "year_data",
                                    "in" : 
                                        {
                                            "k" : "$$year_data.k",
                                            "v" : 
                                                {
                                                    "credit" : "$$year_data.v.Кредитный портфель, млрд руб",
                                                    "deposits" : "$$year_data.v.Депозиты, млрд руб",
                                                    "assets" : "$$year_data.v.Активы банка, млрд руб",
                                                    "equity" : "$$year_data.v.Капитал, млрд руб",
                                                    "profit" : "$$year_data.v.Чистая прибыль, млрд руб",
                                                }
                                        }
                            }
                        }
                }
            }


def div_filter(Companies):
    collections1 = db["Companies"]
    collections2 = db["Finans_info"]
    Filtred = []
    for i in Companies:
        div_yield = []
        pipeline = [
            {
                "$lookup": {
                    "from": "Companies",
                    "localField": "id", 
                    "foreignField": "_id",
                    "as": "Company"
                }
            },
            {
                "$unwind" : "$Company"
            },
            {
                "$match" : {"Company.name" : i}
            },
            {
                "$project" : 
                    {
                        "document_id" : "$_id",
                        "info_array" : {"$objectToArray" : "$info"}
                    }
            },
            {
                "$project" :
                    {
                        "document_id": 1,
                        "div_metric":
                            {
                                "$map" :
                                    {
                                        "input" : 
                                            { 
                                                "$filter" : 
                                                    {
                                                        "input" : "$info_array",
                                                        "as" : "item",
                                                        "cond" : {"$ne" : ["$$item.k", "Период"]}
                                                    }
                                            },

                                        "as" : "year_data",
                                        "in" : 
                                            {
                                                "k": "$$year_data.k",
                                                "v": 
                                                    {"$convert":
                                                        {
                                                            "input" : {
                                                               "$replaceOne":
                                                                {
                                                                    "input" : "$$year_data.v.Див доход, ао, %",
                                                                    "find" : "%",
                                                                    "replacement" : ""
                                                                } 
                                                            },
                                                            "to" : "double",
                                                            "onError" : 0.0,
                                                            "onNull" : 0.0
                                                        }
                                                    }
                                            }
                                    }
                            }
                    }
            },
            {
                "$project" : {"_id": 0, "div" : '$div_metric'}
            }

        ]
        collection = list(collections2.aggregate(pipeline))[0]['div']
        filtred_div = np.array([i['v'] for i in collection])
        if not np.any(filtred_div == 0.0):
            Filtred.append(i)
    return Filtred

def init_sector(Company):
    collections1 = db["Companies"]
    return collections1.find_one({'name':Company})['sector']

def avg_metric(sector, metric):
    collection1 = db["Companies"]           
    collection2 = db["Finans_info"]
    pipeline = [
        {
            "$lookup" : 
                { 
                    "from" : "Companies",
                    "localField" : "id",
                    "foreignField" : "_id",
                    "as" : "Company"
                }
        },
        {
            "$match" : 
                {
                    "Company.sector" : sector
                }
        },
        {
            "$project" : 
                {
                    "_id" : 0,
                    "years" : {"$objectToArray" : "$info"}
                }           
        },
        {
            "$project" : 
                {
                    "_id" : 0,
                    "years" : 
                        {
                            "$filter" : 
                                {
                                    "input" : "$years",
                                    "as" : "y",
                                    "cond" : {"$regexMatch": {"input" : "$$y.k", "regex" : "^[0-9]{4}$"}}
                                }
                        }
                }
        },
        {
            "$project" : 
                {
                    "_id" : 0,
                    "last" : 
                        {
                            "$arrayElemAt" : [
                                {"$sortArray" : {"input" : "$years", "sortBy" : {"k" : -1}}},
                                0
                            ]
                        }
                }
        },
        build_rawvalue_project(metric),
        {
            "$project" : 
                {
                    "_id" : 0,
                    "value" : 
                        {
                            "$cond" : 
                            [
                                {"$eq" : ["$rawValue", None]},
                                None,
                                {
                                    "$toDouble": 
                                        {
                                            "$replaceAll" : 
                                                {
                                                    "input" : 
                                                        {
                                                            "$replaceAll" : 
                                                                {
                                                                    "input" : "$rawValue",
                                                                    "find" : "%",
                                                                    "replacement" : ""
                                                                }
                                                        },
                                                    "find" : " ",
                                                    "replacement" : ""
                                                }
                                        }
                                }
                            ]

                        }
                }
        },
        {
            "$match" : {"value" : {"$ne" : None}}
        },
        {
            "$group" : 
                {
                    "_id" : 0,
                    "values" : {"$push" : "$value"}
                }
        }

    ]
    return np.median(list(collection2.aggregate(pipeline))[0]["values"])

def build_rawvalue_project(metric):
    if metric == "ROE":
        return {
            "$project": {
                "_id": 0,
                "rawValue": {
                    "$ifNull": [
                        "$last.v.ROE, %",
                        "$last.v.Рентабельность банка, %"
                    ]
                }
            }
        }
    else:
        return {
            "$project": {
                "_id": 0,
                "rawValue": f"$last.v.{metric}"
            }
        }
    
def ts_tickers(): 
    collection = db["prices"]
    return list(collection.distinct("ticker"))

def ticker_to_name():
    collection = db["Companies"]
    findes = ts_tickers()
    result = list(collection.find(
        {"ticker" : {"$in" : findes}},
        {"name" : 1, "ticker" : 1, "_id" : 0}
    ))
    return [i.get("name") for i in result]


def save_prices(data):

    collection = db["prices_1m"]
    ts = data["date"]
    ops = [
        InsertOne({
            "meta" : {
                "ticker" : ticker,
                "tf" : "1m"
            },
            "price": float(price),
            "timestamp": ts
        })
        for ticker, price in data.items()
        if ticker != "date"
    ]
    if ops:
        collection.bulk_write(ops, ordered=False)

def delete_zero():
    collection = db["prices_1m"]
    collection.delete_many({"price" : 0})

def floor_time(dt, delta):
    return dt - (dt - dt.min) % delta

def floor_to_interval(dt: datetime, interval_min: int) -> datetime:
    """Округляет вниз до начала интервала (например 5 минут)"""
    minutes = (dt.minute // interval_min) * interval_min
    return dt.replace(minute=minutes, second=0, microsecond=0)


def save_5m_candles(data: dict, target_collection, min_fill_ratio: float = 0.6):
    """
    Принимает данные в формате:
        {
            "date": datetime,
            "SBER": 315.20,
            "GAZP": 142.50,
            ...
        }

    Агрегирует их в 5-минутные свечи (OHLC) и вставляет в time-series коллекцию,
    только если свеча ещё не существует и заполнена достаточно.
    """
    ts = data["date"]                   # это время замера цен
    interval_min = 5
    interval = timedelta(minutes=interval_min)

    # Вычисляем, к какой свече относится этот timestamp
    # bucket_start — начало 5-минутного интервала
    bucket_start = ts.replace(
        minute=(ts.minute // interval_min) * interval_min,
        second=0,
        microsecond=0
    )
    # время закрытия свечи
    bucket_close = bucket_start + interval

    # Если текущее время ещё не дошло до конца свечи → ничего не делаем
    if ts < bucket_close:
        # свеча ещё не закрылась — ждём следующего вызова
        return 0

    # Собираем все цены по тикерам для этой свечи
    buckets = defaultdict(list)

    for ticker, price in data.items():
        if ticker == "date":
            continue
        if not isinstance(price, (int, float)) or price <= 0:
            continue
        buckets[ticker].append(price)

    ops = []
    inserted = 0

    for ticker, prices in buckets.items():
        if len(prices) < interval_min * min_fill_ratio:
            continue  # слишком мало точек

        prices_sorted = sorted(prices)  # на всякий случай, хотя обычно порядок сохраняется

        doc = {
            "timestamp": bucket_close,
            "meta": {
                "ticker": ticker,
                "tf": "5m"
            },
            "open": float(prices_sorted[0]),
            "high": float(max(prices_sorted)),
            "low": float(min(prices_sorted)),
            "close": float(prices_sorted[-1]),
            "filled_count": len(prices_sorted),
            "filled_ratio": round(len(prices_sorted) / interval_min, 3),
        }

        ops.append(InsertOne(doc))

    if not ops:
        return 0

    try:
        result = target_collection.bulk_write(ops, ordered=False)
        inserted = len(result.inserted_ids) if hasattr(result, 'inserted_ids') else len(ops)
        # inserted_count = result.inserted_count  — в новых версиях pymongo
    except Exception as e:
        # можно добавить logging
        print(f"Ошибка bulk_write 5m: {e}")
        return 0

    print("Пятиминутная агрегация")
    return inserted

# def create_col():
#     db.create_collection(
#     "prices_1m",
#     timeseries={
#         "timeField": "timestamp",
#         "metaField": "meta",
#         "granularity": "minutes"
#     }
# )
