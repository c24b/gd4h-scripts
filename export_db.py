#!/usr/bin/env/python3

__doc__ == "Script pour exporter les données en s'appuyant sur les modèles et les règles"

import os
import csv
from utils import DB
from utils import data_dir
from utils import cast_type_for_export, is_multilang_model


def export_datasets(lang="fr"):
    """
    export
    """
    datasets_list = []
    for dataset in DB.datasets.find({}, {"_id": 0}):
        dataset_row = {}
        for k, v in dataset.items():
            rules = DB.rules.find_one({"model": "dataset", "slug": k}, {"_id": 0})
            if rules is not None and "comment" not in k:
                if rules["datatype"] == "bool":
                    dataset_row[k] = v
                    continue
                elif (
                    rules["datatype"] == "dict"
                    and rules["external_model"] == "organization"
                ):
                    dataset_row[k] = "|".join([n["name"] for n in v])
                    continue
                elif rules["multiple"]:
                    if rules["translation"]:
                        try:
                            dataset_row[k] = "|".join([n["name_" + lang] for n in v])
                        except TypeError:
                            dataset_row[k] = "|".join(
                                [
                                    n["name_" + lang]
                                    for n in v
                                    if n["name_" + lang] is not None
                                    and n["name_" + lang] != ""
                                ]
                            )
                        continue
                    else:
                        dataset_row[k] = "|".join(v)
                        continue
                else:
                    if rules["translation"]:
                        dataset_row[k] = v["name_" + lang]
                        continue
                    else:
                        dataset_row[k] = v
                        continue
        datasets_list.append(dataset_row)
    filename = f"datasets_{lang}.csv"
    filepath = os.path.join(data_dir, "export", filename)
    headers = list(sorted(dataset_row.keys()))
    # with open(filepath, "w") as fd:
    #     csv_writer = csv.DictWriter(fd, headers)
    #     csv_writer.writeheader()
    #     for row in datasets_list:
    #         csv_writer.writerow(row)
    raise DeprecationWarning

def export_organisations(lang="fr"):
    for org in DB.organizations.find({}, {"_id": 0}):
        print(org)
    raise DeprecationWarning

def export_to_csv(model="dataset", lang="fr"):
    if is_multilang_model:
        filename = f"{model}_{lang}.csv"
        
    else:
        filename = f"{model}.csv"
        lang = "en"
    filepath = os.path.join(data_dir, "export", filename)
    key_getter = {
            rule["slug"]: rule[f"name_{lang}"]
            for rule in DB.rules.find(
                {
                    "model": model,
                    "external_model": {"$ne": "comment"},
                    "slug": {"$nin": ["_id", "id", "ID"]},
                    "ITEM_order": {"$ne": -1},
                },
                {"_id": 0},
            )
        }
    
    headers = {v: "" for v in key_getter.values()}
    with open(filepath, "w") as fd:
        csv_writer = csv.DictWriter(fd, fieldnames=headers)
        csv_writer.writeheader()
        
        for model_item in DB[f"{model}s"].find({}, {lang: 1, "_id": 0}):
            model_item = model_item[lang]
            exported_item = {
                key_getter[k]: cast_type_for_export(model, lang, k, model_item[k])
                for k in key_getter.keys()
                if k in model_item and k in key_getter.keys()
            }
            csv_writer.writerow(exported_item)


if __name__ == "__main__":
    # export_to_csv("dataset", "fr")
    # export_to_csv("dataset", "en")
    export_to_csv("organization", "fr")
