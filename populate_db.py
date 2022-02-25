#!/usr/bin/.venv/python3.8

import os
from csv import DictReader, DictWriter
import bleach
import datetime

from utils import (SWITCH_LANGS, AVAILABLE_LANG)
from utils import DB
from utils import data_dir
from utils import (translate, translate_doc)
from utils import cast_type_for_import

def import_organizations(lang="fr"):
    """import_organizations
    insert organization defined by rules into DB.organizations from data/organizations/organizations_fr.csv
    """
    DB.organizations.drop()
    org_doc = os.path.abspath(
        os.path.join(data_dir, "organizations", f"organizations_{lang}.csv")
    )
    print(f"Create Organizations by inserting {org_doc}")
    with open(org_doc, "r") as f:
        reader = DictReader(f, delimiter=",")
        for row in reader:
            org = {"fr": {}, "en": {}}
            org[{lang}] = row
            other_lang = SWITCH_LANGS[{lang}]
            org[other_lang] = translate_doc("organization", org, _from=lang)
            db_org = DB.organizations.insert_one(org)
            # create_logs("admin","create", "organization", True, "OK", scope=None, ref_id=db_org.inserted_id)
    print(DB.organizations.count_documents({}), "organization inserted")

def import_datasets(rebuild=False, lang="fr"):
    datasets_doc = os.path.abspath(
        os.path.join(data_dir, "datasets", f"datasets_{lang}.csv")
    )
    print(f"Create Datasets by inserting {datasets_doc}")
    with open(datasets_doc, "r") as f:
        reader = DictReader(f, delimiter=",")
        for row in reader:
            dataset = {"fr": {}, "en": {}}
            other_lang = SWITCH_LANGS[lang]
            dataset[lang] = {
                k: cast_type_for_import("dataset", k, v) for k, v in row.items()
            }
            dataset[other_lang] = translate_doc("dataset", dataset, _from=lang)
            DB.datasets.insert_one(dataset)
    print(DB.datasets.count_documents({}), "datasets")

def register_dataset_comments():
    dataset = DB.datasets.find_one()
    comments_fields = {"fr."+key: 1 for key in dataset["fr"] if "comment" in key}
    comments_fields["_id"] = 1
    for dataset in DB.datasets.find({}, comments_fields):
        dataset_id = dataset["_id"]
        del dataset["_id"]
        for k, v in dataset.items():
            for c_type, c_value in v.items():
                print(c_type, c_value)
                if c_value is not None or c_value != "":
                    comment_text = {"text":c_value, "user": "admin", "date":datetime.datetime.now()}
                    register_comment(comment_text, "dataset", c_type, dataset_id)

def create_default_users():
    default_users = [
        {
            "email": "constance.de-quatrebarbes@developpement-durable.gouv.fr",
            "first_name": "Constance",
            "last_name": "de Quatrebarbes",
            "username": "c24b",
            "organization": "GD4H",
            "roles": ["admin", "expert"],
            "is_active": True,
            "is_superuser": True,
            "lang": "fr",
        },
        {
            "email": "gd4h-catalogue@developpement-durable.gouv.fr",
            "first_name": "GD4H",
            "last_name": "Catalogue",
            "username": "admin",
            "organization": "GD4H",
            "roles": ["admin", "expert"],
            "is_active": True,
            "is_superuser": True,
            "lang": "fr",
        },
    ]
    _id = DB.users.insert_many(default_users)
    create_logs(
        "admin",
        action="create",
        perimeter="user",
        status=True,
        message="OK",
        scope=None,
        ref_id=_id.inserted_ids,
    )
    pipeline = [{"$project": {"id": {"$toString": "$_id"}, "_id": 0, "value": 1}}]
    DB.users.aggregate(pipeline)
    # print(_id.inserted_ids)
    return _id.inserted_ids

def create_comment(username, text="Ceci est un commentaire test"):
    default_user = DB.users.find_one({"username": username}, {"username": 1, "lang": 1})
    if default_user is None:
        raise Exception(username, "not found")
    default_lang = default_user["lang"]
    clean_text = bleach.linkify(bleach.clean(text))
    alternate_lang = SWITCH_LANGS[default_lang]
    alternate_text = translate(clean_text, _from=default_lang)
    comment = {
        "name_" + default_lang: clean_text,
        "name_" + alternate_lang: alternate_text,
        "date": datetime.datetime.now(),
        "user": default_user["username"],
    }
    return comment

def register_comment(comment, perimeter, scope, ref_id, lang="fr"):
    print(comment)
    comment["perimeter"] = perimeter
    comment["scope"] = scope  # field
    comment["ref_id"] = ref_id
    comment["lang"] = lang
    DB.comments.insert_one(comment)

def create_logs(
    username, action, perimeter, status=True, message="OK", scope=None, ref_id=None
):
    default_user = DB.users.find_one({"username": username}, {"username": 1})
    default_lang = "en"
    default_name = "name_en"
    ref_perimeter = DB["ref_perimeter"].find_one({default_name: perimeter}, {"_id": 0})
    ref_action = DB["ref_action"].find_one({default_name: perimeter}, {"_id": 0})
    try:
        assert ref_perimeter is not None
    except AssertionError:
        raise ValueError(f"Log has no perimeter :'{perimeter}'")
        assert ref_action is not None
    except AssertionError:
        raise ValueError(f"Log has no action {perimeter}")
    return {
        "user": username,
        "action": action,
        "perimeter": perimeter,
        "scope": scope,
        "ref_id": ref_id,
        "date": datetime.datetime.now(),
        "status": status,
        "message": message,
    }


def init_data():
    DB.organizations.drop()
    import_organizations()
    DB.datasets.drop()
    import_datasets()
    DB.comments.drop()
    register_dataset_comments()
    create_default_users()
    

if __name__ == "__main__":
    init_data()
