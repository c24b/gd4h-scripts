from pymongo import MongoClient
from elasticsearch7 import Elasticsearch
from elasticsearch7 import exceptions as es_exceptions
import datetime
from .utils import AVAILABLE_LANG as LANGS
from .utils import DB
# import settings
es = Elasticsearch("http://localhost:9200")

def get_indexed_and_facet_fields(model="dataset"):
    return [n["slug"] for n in list(DB.rules.find({"model":model, "$or":[{"is_indexed":True}, {"is_facet":True}]}, {"slug":1}))]

def get_search_rules(model):
    return list(DB.rules.find({"model":model, "$or":[{"is_indexed":True}, {"is_facet":True}]}))

def create_mapping(model="dataset", lang=None):
    map_property = {}
    if lang == "fr":
        analyzer = "std_french"
    else:
        analyzer = "std_english"
    for rules in get_search_rules(model):
        prop_key = rules["slug"]
        if rules["slug"] == "organizations":
            map_property[prop_key] = {"type": "nested"}
        if rules["datatype"] == "object":
            if rules["reference_table"] != "":
                map_property[prop_key] = {"type": "text"}
                map_property[prop_key]["fields"] = {"raw": {"type": "keyword"}}
                map_property[prop_key]["analyzer"] = analyzer     
            elif rules["external_model"] != "":
                map_property[prop_key] = {"type": "nested"} 
            else:
                map_property[prop_key] = {"type": "nested"} 
        elif rules["datatype"] == ["string", "url", "email", "str"]:
            map_property[prop_key] = {"type": "text"}
            map_property[prop_key]["fields"] = {"raw": {"type": "keyword"}}
            map_property[prop_key]["analyzer"] = analyzer 
        elif rules["datatype"] == "date":
            if rules["constraint"] == "range":
                map_property[prop_key]= {"type":"integer_date"}
            else:
                map_property[prop_key] = {"type": "date", "format": "strict_date_optional_time_nanos"}
        elif rules["datatype"] == "boolean":
            map_property[prop_key]= {"type":"boolean"}
        elif rules["datatype"] in ["number", "integer", "int"]:
            if rules["constraint"] == "range":
                map_property[prop_key]= {"type":"integer_range"}
            else:    
                map_property[prop_key]= {"type":"integer"}
        else:
            #map_property[prop_key]= {"type":rules["datatype"]}
            map_property[prop_key] = {"type": "text"}
            map_property[prop_key]["fields"] = {"raw": {"type": "keyword"}}
            map_property[prop_key]["analyzer"] = analyzer
    print(map_property)
    return map_property

def delete_index(model, lang=None):
    if lang is None:
        index_name = f"{model}"
    else:    
        index_name = f"{model}_{lang}"
    es.indices.delete(index=index_name, ignore=[400, 404])
def create_index(model="dataset", lang=None):
    if lang is None:
        index_name = f"{model}"
    else:    
        index_name = f"{model}_{lang}"
    print(f"Creating {index_name}")
    settings = {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "std_english": {
                        "type": "standard",
                        "stopwords": "_english_"
                        },
                        "std_french": {
                        "filters": [
                            "standard",
                            "lowercase",
                            "french_ellision",
                            "icu_folding"
                        ],
                        "type": "custom",
                        "stopwords": "_french_",
                        "tokenizer": "icu_tokenizer"
                        }
                    }
                }            
    }
    config = {"settings": settings, "mappings": {"properties":create_mapping(model, lang)}}
    mappings = {"properties": create_mapping(lang)}
    i = es.indices.create(index=index_name, settings=config["settings"], mappings=config["mappings"], ignore=400)
    print(i)
    #print(f"Created index {i['index']}: {i['acknowledged']}")
    #return i["acknowledged"]
    return(i)

def index_documents(model="dataset", lang=None):
    if lang is None:
        index_name = f"{model}"
    else:    
        index_name = f"{model}_{lang}"
    col_name = f"{model}s"
    print(f"Indexing {col_name} from DB into {index_name}")
    fields = get_indexed_and_facet_fields(model)
    if lang is None:
        display_fields = {f"{f}":1 for f in fields}    
    else:
        display_fields = {f"{lang}.{f}":1 for f in fields}
    display_fields["_id"] = 1
    errors = 0
    success = 0
    doc_to_index = DB[col_name].count_documents({})
    for doc in DB[col_name].find({}, display_fields):
        doc_id = str(doc["_id"])
        index_doc = doc[lang]
        index_doc["id"] = doc_id
        try:
            index_doc["organizations"] = [{**{"id":str(o["_id"])}, **o[lang]} for o in index_doc["organizations"] if o is not None]
        except KeyError:
            pass
        
        try:
            response = es.index(index = index_name,id = doc_id, document = index_doc,request_timeout=45)
            # print(response)
            success+=1
        except es_exceptions.RequestError as e:
            errors+=1
            print(doc)
    doc_indexed = es.count({})
    print(doc_indexed["count"], "/", doc_to_index)
    print(f"success: {success} errors: {errors}")       

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

def setup_indexes():
    for model in ["dataset", "organization"]:
        for lang in LANGS:
            create_index(model, lang)
def delete_indexes():
    for model in ["dataset", "organization"]:
        for lang in LANGS:
            delete_index(model, lang)
def delete_index_documents():
    for model in ["dataset", "organization"]:
        for lang in LANGS:
            delete_documents(model, lang)
def delete_documents(model, lang=None):
    if lang is None:
        index_name = f"{model}"
    else:    
        index_name = f"{model}_{lang}"
    es.delete_by_query(index=[index_name], body={"query": {"match_all": {}}})

def populate_indexes():
    for lang in LANGS:
        for model in ["dataset", "organization"]:
            index_documents(model, lang)    

def init_indexation():
    delete_indexes()
    setup_indexes()
    # delete_index_documents()
    populate_indexes()

if __name__=="__main__":
    init_indexation()