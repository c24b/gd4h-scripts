import os
import json
import itertools
from jsonschema_to_openapi.convert import convert
from pydantic import create_model
from rules_utils import (get_mandatory_fields, get_rules)
from utils import data_dir, schema_dir
from utils import DB
from utils import get_json_ref_type, get_json_type, 

def create_json_schema_from_example(model):
    model_name = model.title()
    example = DB[model+"s"].find_one({"$nin": ["_id", "id"]}, {"_id":0})
    if example is not None:
        rule_model = create_model(model_name, **example)
        with open(os.path.join(data_dir, "schemas", f"{model_name}.json"), "w") as f:
            f.write(rule_model.schema_json())

def create_rules_json_schema():
    '''create json schema using example loaded into pydantic.create_model'''
    rule_model = create_model("Rules", **DB.rules.find_one({},{"_id":0}))
    rule_json_schema = rule_model.schema_json()
    with open(os.path.join(data_dir, "schemas", "rules.json"), "w") as f:
        f.write(rule_json_schema)
   

def create_reference_json_schema():
    '''create json schema using example loaded into pydantic.create_model'''
    rule_model = create_model("Reference", **DB.references.find_one({},{"_id":0}))
    rule_json_schema = rule_model.schema_json()
    with open(os.path.join(schema_dir, "references.json"), "w") as f:
        f.write(rule_json_schema)


def create_json_schema(model_name, model_name_title, model_rules, lang):
    '''Given rules for one model build a jsonschema dict'''
    if model_name not in DB.rules.distinct("model"):
        raise NameError("Model: {} doesn't exists".format(model_name))
    doc_root = { 
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": model_name_title,
        "type": "object",
        "description": f"A {model_name} in the catalog",
        "$id": f"http://gd4h.fr/schemas/{model_name}.json",
        "properties": {}
    }
    doc_root["properties"] = {}
    # doc_root["definitions"] = {name.title(): {} for name in DB.rules.distinct("modele") if name != model_name}
    for field in model_rules:
        
        doc_root["properties"][field["slug"]] = {
            "title": field[f"name_{lang}"].title(), 
            "description": field[f"description_{lang}"]    
            }       
        field_type = get_json_type(field)
        if field["multiple"]:
            doc_root["properties"][field["slug"]]["type"] = "array" 
            doc_root["properties"][field["slug"]]["items"] = get_json_type(field)

        else:
            doc_root["properties"][field["slug"]].update(field_type)
            # doc_root["properties"][field["slug"]].udpate(field_type)
    doc_root["definitions"]= { 
        field["slug"].title():{
            "properties": {
                # f["slug"]: get_json_type(f)
                f["slug"]: get_json_ref_type(f, field) 
                for f in get_rules(field["external_model"])
            }
        }  for field in model_rules if field["external_model"]!=""
    }
    doc_root["required"] = get_mandatory_fields(model_name)
                           
    # with open(os.path.join(schema_dir, f"{model_name_title}.json"), "w") as f:
    #     data = json.dumps(doc_root, indent=4)
    #     f.write(data)
    # return os.path.join(schema_dir, f"{model_name_title}.json")
    return doc_root
def save_json_schema(model_name, json_data):
    '''savec generated json_schema into data/schema/'''
    json_filepath = os.path.join(schema_dir, f"{model_name.title()}.json")
    with open(json_filepath, "w") as f:
        data = json.dumps(json_data, indent=4)
        f.write(data)
    return json_filepath
def convert_jsonschema_to_openapi(model_name, json_data):
    return convert(json_data)


def create_json_schemas():
    '''from model list in rules 
    - generate json_schema
    - import pydantic_model
    '''
    models = {}
    for model_name in DB.distinct("model"):
        model_rules = get_rules(model_name)
        is_multilang_model = any([r["translation"] for r in model_rules])
        
        if is_multilang_model:
            for lang in AVAILABLE_LANG:
                model_name_title = model_name.title()+ lang.upper()
                
                json_schema = create_json_schema(model_name, model_name_title, model_rules, lang)
                save_json_schema(model_name, json_schema)
                py_model = create_model(model_name.title(), **json_schema)
                models[model_name+"_"+lang] = py_model
                # model_py = create_model(model_name_title, **json_schema)
                
                # yield create_json_schema(model_name, model_name_title, model_rules, lang)

    else:
        model_name_title = model_name.title()
        json_schema = create_json_schema(model_name, model_name_title, model_rules, lang="fr")
        model_py = create_model(model_name_title, **json_schema)
        save_json_schema(model_name, json_schema)
        models[model_name+"_"+lang] = py_model
        yield(model_name_title, model_py)

def create_json_model():
    '''generate JSON empty file from rules table'''
    rules = [rule for rule in DB["rules"].find({},{})]

    final_models = {}
    for  model_name, field_rules in itertools.groupby(rules, key=lambda x:x["model"]):
        print(model_name)
        field_rules = list(field_rules)
        is_multilang_model = any([r["translation"] for r in field_rules])
        if is_multilang_model:
            for lang in AVAILABLE_LANG:
                root_schema = {
                "$schema": "https://frictionlessdata.io/schemas/table-schema.json",
                "name": f"{model_name}_{lang}",
                "title": "{} - {}".format(model_name.title(), lang),
                "description": f"Spécification du modèle de données {model_name} relatif au catalogue des jeux de données Santé Environnement du GD4H",
                "contributors": [
                    {
                    "title": "GD4H",
                    "role": "author"
                    },
                    {
                    "title": "Constance de Quatrebarbes",
                    "role": "contributor"
                    }
                ],
                "version": "0.0.1",
                "created": "2022-01-28",
                "lastModified": "2022-01-28",
                "homepage": "https://github.com/c24b/gd4h/",
                "$id": f"{model_name}_{lang}-schema.json",
                "path": f"https://github.com/c24b/gd4h/catalogue/raw/v0.0.1/{model_name}_{lang}-schema.json",
                "type":object,
                "properties":[

                ]
                }
                for rule in field_rules:
                    root_schema["properties"].append(gen_json_item(rule, lang))
                yield root_schema
                root_schema["properties"] = []
        else:
            root_schema = {
                "$schema": "https://frictionlessdata.io/schemas/table-schema.json",
                "name": f"{model_name}",
                "title": f"{model_name.title()} Simplifié",
                "description": f"Spécification du modèle de données {model_name.title()} relatif au catalogue des jeux de données Santé Environnement du GD4H",
                "contributors": [
                    {
                    "title": "GD4H",
                    "role": "author"
                    },
                    {
                    "title": "Constance de Quatrebarbes",
                    "role": "contributor"
                    }
                ],
                "$id": f"{model_name}-schema.json",
                "version": "0.0.1",
                "created": "2022-01-28",
                "lastModified": "2022-01-28",
                "homepage": "https://github.com/c24b/gd4h/",
                "path": f"https://github.com/c24b/gd4h/catalogue/raw/v0.0.1/{model_name}-schema.json".format(model_name),
                "properties":[

                ]
            }
            for rule in field_rules:
                root_schema["properties"].append(gen_json_item(rule, "en"))
            yield root_schema
    
def write_json_model():
    for json_schema in create_json_model():
        with open(os.path.join(data_dir, "schemas", json_schema["$id"]), "w") as f:
            f.write(json.dumps(json_schema, indent=4))
            print(json_schema["$id"], "ready!")
