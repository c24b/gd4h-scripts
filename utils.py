#!/usr/bin/.venv/python3.8
# file: utils.py

import os
import datetime
from pymongo import MongoClient
from csv import DictReader, DictWriter
import json
from argostranslate import package, translate

DATABASE_NAME = "GD4H_V2"
mongodb_client = MongoClient("mongodb://localhost:27017")
DB = mongodb_client[DATABASE_NAME]

AVAILABLE_LANG = ["fr","en"]
SWITCH_LANGS = dict(zip(AVAILABLE_LANG,AVAILABLE_LANG[::-1]))

installed_languages = translate.get_installed_languages()
fr_en = installed_languages[1].get_translation(installed_languages[0])
en_fr = installed_languages[0].get_translation(installed_languages[1])

curr_dir = os.path.dirname(os.path.abspath(__file__))
import os
from argostranslate import package, translate
from pymongo import MongoClient

# from rules_utils import (get_rule, get_multiple_fields) 
# from rules_utils import (get_not_translated_fields, get_fields_to_translate, get_reference_fields, get_reference_name_lang)


DATABASE_NAME = "GD4H_V2"
mongodb_client = MongoClient("mongodb://localhost:27017")
DB = mongodb_client[DATABASE_NAME]

AVAILABLE_LANG = ["fr","en"]
SWITCH_LANGS = dict(zip(AVAILABLE_LANG,AVAILABLE_LANG[::-1]))

installed_languages = translate.get_installed_languages()
fr_en = installed_languages[1].get_translation(installed_languages[0])
en_fr = installed_languages[0].get_translation(installed_languages[1])

parent_dir = os.path.dirname(curr_dir)
data_dir = os.path.join(parent_dir, "data")
meta_dir = os.path.join(data_dir, "meta")
schema_dir = os.path.join(data_dir, "schemas")

# TRANSLATION
def translate(text, _from="fr"):
    if _from == "fr":
        return fr_en.translate(text)
    else:
        return en_fr.translate(text)


def translate_doc(model, doc, _from="fr"):
    '''
    _from=fr > en
    _from=en > fr
    '''
    other_lang = SWITCH_LANGS[_from]
    _from_doc = doc[_from]
    to_translate = {}
    for key, value in _from_doc.items():
        if key not in DB.rules.distinct("slug"):
            to_translate[key] = value
            continue
        elif key in get_multiple_fields(model):
            if key in get_not_translated_fields(model):
                to_translate[key] = value
            elif key in get_fields_to_translate(model):
                to_translate[key] = [translate(v,_from) for v in value]
            else:
                assert key in get_reference_fields(model), key
                to_translate[key] = [get_reference_name_translated(key, v, _from) for v in value]
                
                
        else:
            if key in get_not_translated_fields(model):
                to_translate[key] = value
            elif key in get_fields_to_translate(model):
                to_translate[key] = [translate(value,_from)]
            else:
                assert key in get_reference_fields(model), key
                to_translate[key] = get_reference_name_translated(key, value)
                

    return to_translate

def get_not_translated_fields(model="organization"):
    '''get all fields for given model that are not multilingual'''
    fields = DB.rules.find({"model": model, "translation": False}, {"slug":1})
    return [f["slug"] for f in fields]

def get_fields_to_translate(model="dataset"):
    fields = DB.rules.find({"model": model, "translation": True, "reference_table":"", "is_controled":False}, {"slug": 1})
    return [f["slug"] for f in fields]

def get_translated_fields(model="organization"):
    '''get all fields for given model that are multilingual'''
    fields = DB.rules.find({"model": model, "translation": True}, {"slug":1})
    return [f["slug"] for f in fields]

def get_reference_name_translated(field, value, _from="fr"):
    if value != "":
        other_lang = SWITCH_LANGS[_from]
        ref = DB["ref_"+field].find_one({f"name_{_from}":value})
        if ref is not None:
            return ref[f"name_{other_lang}"]
    return value
    # refs = DB.references.find_one({"slug":field, "refs":{"$elemMatch":{f"name_{_from}":value}}},{"refs.$":1, "_id":0})
    # if refs is not None:
    #     return refs["refs"][0][f"name_{other_lang}"] 
    # else:
    #     return 
    # ref = DB[f"ref_{field}"].find_one({f"name_{_from}": value})
    # trad = ref[f"name_{other_lang}"]
    # print("ref", field, value, trad)
    # return ref[f"name_{other_lang}"]

# FIELDS RULES
def get_rule(model, field_slug):
    '''get rule for given model and given field'''
    rule = DB.rules.find_one({"model": model, "slug": field_slug})
    return rule

def get_rules(model):
    '''get rule for given model and given field'''
    return list(DB.rules.find({"model": model},{"_id":0}))

def get_multiple_fields(model="dataset"):
    fields = DB.rules.find({"model": model, "multiple": True}, {"slug": 1})
    return [f["slug"] for f in fields]

def get_mandatory_fields(model="organization"):
    fields = DB.rules.find({"model": model, "mandatory": True}, {"slug":1})
    return [f["slug"] for f in fields]

def get_searchable_fields(model):
    fields = DB.rules.find({"model": model, "$or":[{"indexed": True},{"is_facet": True}]}, {"slug":1})
    return [f["slug"] for f in fields]

def get_indexed_fields(model):
    fields = DB.rules.find({"model": model, "indexed": True}, {"slug":1})
    return [f["slug"] for f in fields]

def get_facet_fields(model):
    fields = DB.rules.find({"model": model, "is_facet": True}, {"slug":1})
    return [f["slug"] for f in fields]

def get_reference_fields(model):
    fields = DB.rules.find(
        {"model": model, "reference_table": {"$ne": ""}}, {"slug": 1}
    )
    return [f["slug"] for f in fields]

def get_reference_values(model, lang):
    references = {}
    for field in get_reference_fields(model):
        references[field] = []
        for x in DB.references.find({"slug": field}):
            for y in x["refs"]:
                references[field].append(y[f"name_{lang}"])
    return references

def get_json_type(rule):
    '''
    given key 'datatype' in rule record transform it into jsonschema datatype  
    given key 'constraint' in rule record add constraint
    '''
    datatype = {}
    if rule["constraint"] == "unique":
        datatype["unique"] = True
    elif rule["constraint"] == "if_exist":
        datatype["type"] =  [datatype["type"], "null"]
    if rule["datatype"] == "date":
        datatype["type"] = "string"
        datatype["format"] = "date-time"
        return datatype
    elif rule["datatype"] == "id":
        datatype["type"] = "string"    
        return datatype
    elif rule["datatype"] == "url":
        datatype["type"] = "string"
        datatype["format"] = "uri"
        datatype["pattern"] = "^https?://"
        return datatype
    elif rule["datatype"] == "object":
        datatype["$ref"] = f"#/definitions/{rule['slug'].title()}"
        return datatype
    else:
        datatype["type"] = rule["datatype"]
    return datatype

def get_json_ref_type(rule, ref_rule):
    ''' special case for external models 
    given rule 
    - if rule model is a reference and slug is name_fr or name_en or uri
    add enum validation and type
    - if rule model is an external model refer to external schema
    '''
    datatype = {}
    if rule["model"] == "reference" and rule["slug"] in ["name_fr", "name_en", "uri"]:
        if rule["slug"] == "uri":
            datatype["type"] = "string"
            datatype["format"] = "uri"
            datatype["pattern"] = "^https?://"
        else:
            datatype["type"] = "string"
        datatype["enum"] = DB[ref_rule["reference_table"]].distinct(rule["slug"])
        if len(datatype["enum"]) == 0:
            del datatype["enum"]
        return datatype
    elif rule["model"] != "":
        if rule["datatype"] == "object":
            datatype["$ref"] = f"#/schemas/{rule['slug'].title()}.json"
            return datatype
    else:
        return get_json_type(rule)






def is_multilang_model(model_name):
    """define if the model has two langs"""
    return any(
        [
            r["translation"]
            for r in DB["rules"].find(
                {"model": model_name}, {"translation": 1, "_id": 0}
            )
        ]
    )


def is_search_model(model_name):
    """define if the model has to be searchable"""
    return any(
        [
            r["is_indexed"]
            for r in DB["rules"].find(
                {"model": model_name}, {"is_indexed": 1, "_id": 0}
            )
        ]
    )


def is_facet_model(model_name):
    """define if the model has to be filtered"""
    return any(
        [
            r["is_facet"]
            for r in DB["rules"].find({"model": model_name}, {"is_facet": 1, "_id": 0})
        ]
    )

if __name__ == "__main__":
    # dataset = DB.datasets.find_one()
    # en = translate_doc("dataset", dataset)
    # print(en)
    pass