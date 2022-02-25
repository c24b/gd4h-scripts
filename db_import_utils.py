#!/usr/bin/env/python3

__doc__ == "Script pour importer les données depuis un csv puis les données en s'appuyant sur les modèles et les règles"

import os
import csv
from utils import SWITCH_LANGS, AVAILABLE_LANG
from utils import DB
from utils import data_dir
import os
from csv import DictReader, DictWriter
import bleach
import datetime
import pymongo

from utils import (SWITCH_LANGS, AVAILABLE_LANG)
from utils import DB
from utils import data_dir
from utils import is_multilang_model
from utils import translate, translate_doc
from utils import get_rule


def cast_type_for_csv_import(model, field, value):
    rule = get_rule(model, field)
    if rule is None:
        if "|" in value:
            rule = {"multiple": True}
        else:  
            if value in ["true", "True"]:
                return True
            if value in ["false", "False"]:
                return False
            if value in ["NA", "None", "NULL"]:
                return None
            else:
                return str(value)
    if rule["multiple"]:
        return [cast_value_for_csv_import(rule["datatype"], v) for v in value.split("|")]
    else:
        return cast_value_for_csv_import(rule["datatype"], value)


def cast_value_for_csv_import(datatype, value):
    if value is None:
        return None
    if value in ["None","NULL", "NA"]:
        return None
    if value == "":
        return ""
    if datatype == "date":
        #2021-01-01T00:04:00.000Z
        return datetime.datetime(**value.split(".")[0])
    if datatype == "boolean":
        return bool(value)
    # if datatype == "date":
    #     #2021-01-01T00:04:00.000Z
    #     return value.split(".")[0]
    if datatype == "int":
        try:
            return int(value)
        except:
            return str(value)
    if datatype == "object":
        return value

    return str(value)
    
def import_rules_from_csv():
    '''
    import_rules into DB['rules'] from data/import/rules.csv
    '''
    coll_name = "rules"
    DB[coll_name].drop()
    rules_doc = os.path.join(data_dir, "import", "rules.csv")
    print(f"Creating {coll_name} table")
    with open(rules_doc, "r") as f:
        reader = DictReader(f, delimiter=",")
        for row in reader:
            for k,v in row.items():
                # if row["multiple"] == "True":
                if "|" in v:
                    row[k] == v.split("|")
                elif k == "external_model_display_keys":
                    row[k] == v.split("|")
                elif v == "True":
                    row[k] = True
                elif v == "False":
                    row[k] = False
                else:
                    try:
                        row[k] = int(v)
                    except ValueError:
                        row[k] = str(v)
            _id = DB[coll_name].insert_one(row).inserted_id

def import_references_from_csv():
    assert len(DB.rules.distinct("reference_table")) > 0, "Error: no rules specified. A rules table is required"
    DB.references.drop()
    for ref_table in DB.rules.distinct("reference_table"):
        if ref_table != "":
            DB[ref_table].drop()
            print(f"Creating {ref_table} table")
            meta_reference = {"model":"reference"}
            meta_reference["table_name"] = ref_table
            meta_reference["slug"] =  ref_table.replace("ref_", "")
            ref_file =  os.path.join(data_dir, "import", ref_table+".csv")
            meta_reference["refs"] = [] 
            try:
                with open(ref_file, "r") as f:            
                    reader = DictReader(f, delimiter=",")
                    
                    for row in reader:
                        clean_row = {k.strip(): v.strip() for k,v in row.items() if v is not None}
                        try:
                            if clean_row["name_en"] != "" and clean_row["name_fr"] == "":
                                clean_row["name_fr"] = translate(clean_row["name_en"], _from="en")
                            if clean_row["name_fr"] != "" and clean_row["name_en"] =="":
                                clean_row["name_en"] = translate(clean_row["name_fr"], _from="fr")
                        except KeyError:
                            print("Error in clean_row not name_*", clean_row.keys())
                            pass
                        
                        if "root_uri" in clean_row:
                            meta_reference["root_uri"] = row["root_uri"]
                        if "root_uri" not in clean_row and "uri" in clean_row:
                            if clean_row["uri"] != "":
                                meta_reference["root_uri"] = "/".join(row["uri"].split("/")[:-1])
                        meta_reference["refs"].append(clean_row)            
                        try:
                            ref_id = DB[ref_table].insert_one(clean_row)
                        except pymongo.errors.DuplicateKeyError:
                            pass
                        del clean_row
                with open(ref_file, "w") as f:        
                    one_record = DB[ref_table].find_one({}, {"_id":0})
                    if one_record is not None:
                        fieldnames = list(one_record.keys())
                        writer = DictWriter(f, delimiter=",",fieldnames=fieldnames)
                        writer.writeheader()
                        for row in DB[ref_table].find({}, {"_id":0}):
                            writer.writerow(row)
                meta_reference["status"] = True
            except FileNotFoundError:
                print(f"Error! required reference {ref_table} has no corresponding file {ref_file}")
                meta_reference["status"] = False
            try:
                DB.references.insert_one(meta_reference)
            except pymongo.errors.DuplicateKeyError:
                print("Err")
                pass
    print("Created references table")

def import_model_from_csv(model, lang, import_dir = os.path.join(data_dir, "import")):
    '''Import a model from a csv following rules'''
    assert len(DB.rules.distinct("reference_table")) > 0, "Error: no rules specified pleas build_rules before launching references"
    assert len(DB.rules.distinct("reference_table")) > 0, "Error: no references specified please build_references before launching references"
    
    if is_multilang_model(model):
        csv_file = os.path.abspath(import_dir, f"{model}_{lang}.csv")    
    else:
        csv_file = os.path.abspath(import_dir, f"{model}.csv")
    
    assert os.file.exists(csv_file), f"Error: no file {csv_file} found"
    
    print(f"Create {model}s by inserting {csv_file}")
    with open(csv_file, "r") as f:
        reader = DictReader(f, delimiter=",")
        for row in reader:
            if is_multilang_model(model):
                dataset = {"fr": {}, "en": {}}
                other_lang = SWITCH_LANGS[lang]
                dataset[lang] = {
                    k: cast_type_for_csv_import(model, k, v) for k, v in row.items()
                }
                dataset[other_lang] = translate_doc(model, dataset, _from=lang)
            else:
                lang = "en"
                dataset = {
                    cast_type_for_csv_import(model, k, v) for k, v in row.items()
                }
            DB[f'{model}s'].insert_one(dataset)
    print(DB[f"{model}s"].count_documents({}), f"{model}s from {csv_file}")
    

def build_import_file_template(model="dataset", lang="fr"):
    """
    build a template file to add multiple model
    """
    if is_multilang_model(model):
        filename = os.path.abspath(
            os.path.join(data_dir, "import", f"template-{model}_{lang}.csv")
        )
    else:
        filename = os.path.abspath(
            os.path.join(data_dir, "import", f"template-{model}.csv")
        )
    headers = {
        rule[f"name_{lang}"]: ""
        for rule in DB.rules.find({"model": model}, {"_id": 0})
        if rule["external_model"] != "comment"
        and rule["slug"] not in ["_id", "id", "ID"]
        and rule["ITEM_order"] != -1
    }
    with open(filename, "w") as fd:
        csv_writer = csv.DictWriter(fd, headers)
        csv_writer.writeheader()


if __name__ == "__main__":
    import_rules_from_csv()
    import_references_from_csv()
    build_import_file_template()
    
    # # for model_name in DB["rules"].distinct("model"):
    # #     if model not in ["rule", "reference", "comment", "log", "user", "role"]:
    # for model in ["dataset", "organization", "user"]:
    #     for lang in AVAILABLE_LANG:
    #         import_model_from_csv(model, lang)
            
