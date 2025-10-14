from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config import MONGODB_URI
from config import DB_NAME

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

def insert_compains(data):
    companies_collection = db['Companies']
    company_data = {
        "id" : data["EMITTER_ID"],
        "name" : data["NAME"],
        "sector" : data["SECTOR"],
        "inn" : data["inn"],
        "okpo" : data["okpo"],
        "cap": data["Capitalization"]

    }
    result = companies_collection.insert_one(company_data)

def inser_info(tic, data):
    info_collection = db["Finans_info"]
    info_collection.insert_one(data)

def update_compains_info(data, id):
    companies_collection = db['Companies']
    for key, value in data.items():
        companies_collection.update_one(
            {"id" : id},
            {"$set": {key : value}}
        )

def find_info(em_id):
    collections = db["Finans_info"]
    docs = collections.find({
        em_id : {'$exists':True},
        em_id : {'$type': 'object'}
    })
    return docs

def get_all_em_id():
    collections = db["Companies"]
    docs = collections.find({}, {'id': 1})
    result = [i["id"] for i in docs]
    return result

def get_em_name(em_id):
    collections = db["Companies"]
    result = [collections.find_one({'id': i}, {'name' : 1})["name"] for i in em_id]
    return result

def find_id_by_name(name):
    colletions = db["Companies"]
    return colletions.find_one(({'name' : name}))["id"]

def get_base_info(em_id):
    collections = db["Companies"]
    return collections.find_one({'id':em_id})


