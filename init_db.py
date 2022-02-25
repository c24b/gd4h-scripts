#!/usr/bin/env python3

import os
from csv import DictReader, DictWriter

import pymongo

from .utils import translate
from .utils import DB
from .utils import meta_dir, data_dir



def init_meta():
    DB.rules.drop()
    import_rules()
    DB.references.drop()
    import_references()
    


def import_rules():
    '''
    import_rules() from data/meta/rules.csv
    '''
    coll_name = "rules"
    DB[coll_name].drop()
    rules_doc = os.path.join(meta_dir,"rules.csv")
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
            

def import_references():
    ''' import_references() 
    from db.rules table 
    from data/references/ref_*.csv  
    - import each ref_table from csv into db tables
    - register each ref_table into table references
    - translate missing field with fr or en version
    - update ref_table csv files back again with db
    '''
    DB.references.drop()
    for ref_table in DB.rules.distinct("reference_table"):
        if ref_table != "":
            DB[ref_table].drop()
            print(f"Creating {ref_table} table")
            meta_reference = {"model":"reference"}
            meta_reference["table_name"] = ref_table
            meta_reference["slug"] =  ref_table.replace("ref_", "")
            ref_file =  os.path.join(data_dir, "references", ref_table+".csv")
            meta_reference["refs"] = [] 
            try:
                with open(ref_file, "r") as f:            
                    reader = DictReader(f, delimiter=",")
                    
                    for row in reader:
                        clean_row = {k.strip(): v.strip() for k,v in row.items() if v is not None}
                        # print("translating missing values")
                        
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


def translate_missing_references():
    for tablename in DB.references.distinct("tablename"):
        for ref_value in DB[tablename].find(
            {
                "$or": [{"name_en": ""}, {"name_en": None}],
                "name_fr": {"$nin": ["", None]},
            },
            {"_id": 1, "name_fr": 1},
        ):
            try:
                value_en = translate(ref_value["name_fr"], _from="fr")
                DB[tablename].update_one(
                    {"_id": ref_value["_id"]}, {"$set": {"name_en": value_en}}
                )
            except KeyError:
                pass
    for tablename in DB.references.distinct("tablename"):
        for ref_value in DB[tablename].find(
            {
                "$or": [{"name_fr": ""}, {"name_fr": None}],
                "name_en": {"$nin": ["", None]},
            },
            {"_id": 1, "name_en": 1},
        ):
            try:

                value_fr = translate(ref_value["name_en"], _from="en")
                DB[tablename].update_one(
                    {"_id": ref_value["_id"]}, {"$set": {"name_fr": value_fr}}
                )
            except KeyError:
                pass

if __name__ == '__main__':
    import_rules()
    import_references()
    