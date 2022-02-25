#!/usr/bin/env/python3

__doc__ == "Script pour exporter les données en s'appuyant sur les modèles et les règles"

import os
import csv
from utils import DB
from utils import data_dir
from utils import get_rule, get_rules, is_multilang_model
from db_import_utils import import_rules_from_csv, import_references_from_csv
import datetime


def cast_type_for_csv_export(model, lang, field, value):
    if value is None:
        return ""
    rule = get_rule(model, field)
    datatype = rule["datatype"]
    if rule["multiple"]:
        if rule["external_model"] != "" and rule["reference_table"] == "":
            ext_model = rule["external_model"]
            ext_key = rule["external_model_display_keys"].split("|")[0]

            ext_rule = get_rule(ext_model, ext_key)
            ext_datatype = ext_rule["datatype"]
            if ext_datatype == "id":
                return "|".join(
                    [
                        cast_value_for_csv_export(ext_datatype, v)
                        for v in value
                        if v is not None
                    ]
                )
            elif is_multilang_model(ext_model):
                return "|".join(
                    [
                        cast_value_for_csv_export(ext_datatype, v[lang][ext_key])
                        for v in value
                        if v is not None
                    ]
                )
            else:
                return "|".join(
                    [cast_value_for_csv_export(ext_datatype, v[ext_key]) for v in value]
                )
        else:
            return "|".join(
                [cast_value_for_csv_export(datatype, v) for v in value if v is not None]
            )
    else:
        if rule["external_model"] != "" and rule["reference_table"] == "":
            ext_model = rule["external_model"]
            ext_key = rule["external_model_display_keys"].split("|")[0]
            ext_rule = get_rule(ext_model, ext_key)
            ext_datatype = ext_rule["datatype"]
            if ext_datatype == "id":
                return cast_value_for_csv_export(ext_datatype, value)
            elif is_multilang_model(ext_model):
                # ext_rules = get_rules(ext_model)

                return cast_value_for_csv_export(ext_datatype, value[lang][ext_key])
            else:
                return cast_value_for_csv_export(ext_datatype, value[ext_key])
        else:
            return cast_value_for_csv_export(datatype, value)


def cast_value_for_csv_export(datatype, value):
    if value is None:
        return "None"
    if value == "":
        return ""
    if datatype == "boolean":
        return bool(value)
    if datatype == "date":
        # 2021-01-01T00:04:00.000Z
        return value.split(".")[0]
    if datatype == "object":
        return value
    return str(value)


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
    with open(filepath, "w") as fd:
        csv_writer = csv.DictWriter(fd, headers)
        csv_writer.writeheader()
        for row in datasets_list:
            csv_writer.writerow(row)
    # raise DeprecationWarning


def export_model_to_csv(model="dataset", lang="fr"):
    now = datetime.datetime.now()
    today_now = now.strftime("%Y-%m-%d_%H:%M:%S")
    if is_multilang_model:
        filename = f"{model}_{lang}-{today_now}.csv"

    else:
        filename = f"{model}-{today_now}.csv"
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
                key_getter[k]: cast_type_for_csv_export(model, lang, k, model_item[k])
                for k in key_getter.keys()
                if k in model_item and k in key_getter.keys()
            }
            csv_writer.writerow(exported_item)


if __name__ == "__main__":
    # import_rules_from_csv()
    # import_references_from_csv()
    export_model_to_csv("dataset", "fr")
    export_model_to_csv("dataset", "en")
    # export_model_to_csv("organization", "fr")
    