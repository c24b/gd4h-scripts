from elasticsearch7 import Elasticsearch
from pymongo import MongoClient

#use settings
es = Elasticsearch("http://localhost:9200")
LANGS = ["fr", "en"]
DATABASE_NAME = "GD4H_V2"
mongodb_client = MongoClient("mongodb://localhost:27017")
DB = mongodb_client[DATABASE_NAME]

def get_indexed_and_facet_fields(model="{model_name}"):
    return list(DB.rules.find({"model":model, "$or":[{"is_indexed":True}, {"is_facet":True}]}, {"_id":0}))

def get_indexed_fieldnames(model="{model_name}"):
    return [n["slug"] for n in list(DB.rules.find({"model":model,"is_indexed":True}, {"slug":1, "_id":0}))]

def get_facet_fieldnames(model="{model_name}"):
    return [n["slug"] for n in list(DB.rules.find({"model":model,"is_facet":True}, {"slug":1, "_id":0}))]

def index_document(model, doc):
    for lang in LANGS:
        index_name = f"{model}_{lang}"
        fields = get_indexed_and_facet_fields(model)
        fields.append("_id")
        doc_id = str(doc["_id"])
        index_doc = doc[lang]
        index_doc["_id"] = doc_id
        response = es.index(index = index_name,id = doc_id, document = index_doc,request_timeout=45)
        print(response)

def index_documents(model="{model_name}", lang="fr"):
    index_name = f"{model}_{lang}"
    fields = ["_id"]
    col_name = f"{model}s"
    fields = get_indexed_and_facet_fields(model)
    fields.append("_id")
    display_fields = {f:1 for f in fields}
    for doc in DB[col_name].find({}, display_fields):
        doc_id = str(doc["_id"])
        index_doc = doc[lang]
        index_doc["_id"] = doc_id
        response = es.index(index = index_name,id = doc_id, document = index_doc,request_timeout=45)
        print(response)
    return

def delete_document(doc_id, model="{model_name}", lang="fr"):
    index_name = f"{model}_{lang}"
    response = es.remove(index = index_name,id = doc_id,request_timeout=45)
    print(response)
    return
def search_documents(query, highlight, model="{model_name}", lang="fr"):
    index_name = f"{model}_{lang}"
    res = es.search(index=index_name, query=query, highlight=highlight)
    result_count =  res["hits"]["total"]["value"]
    results = []
    for r in res["hits"]["hits"]:
        result = r["_source"]
        result["_id"] = r["_id"]
        result["score"] = str(round(r["_score"]*10,2))+"%"
        # result["highlight"] = r["highlight"]
        results.append(result) 
    return {"results": results, "count": result_count}

def sync_get_filters(lang):
    filters = []
    for facet in DB["rules"].find({"model": "{model_name}", "is_facet": True}):
        filter_d = {
                "name": facet["slug"], 
                "label": facet[f"name_{lang}"],
                "description": facet[f"description_{lang}"],
                "is_controled":facet["is_controled"], 
                "is_multiple":facet["multiple"], 
                "is_bool": facet["datatype"] == "boolean"
            }
        if facet["is_controled"]:
            filter_d["values"] = DB[facet["reference_table"]].distinct(f"name_{lang}")
        elif facet["datatype"] == "boolean":
            filter_d["values"] = [True, False]
        elif facet["slug"] == "organizations":
            filter_d["values"] = [org[lang]["name"] for org in DB["organizations"].find()]
        filters.append(filter_d)
    return filters
