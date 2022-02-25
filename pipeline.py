#!/usr/bin/.venv/python3.8
#!/usr/bin/.venv/python3.8
import os
import shutil
from scripts.utils import *
from scripts.db_import_utils import import_from_csv, import_rules_from_csv, import_references_from_csv, import_model_from_csv
#tmp
from scripts.populate_db import init_data
from scripts.index_utils import init_indexation
from scripts.generate_api import generate_app

# from scripts.setup_index import create_index
# from scripts.setup_index import index_documents
from scripts.setup_index import init_indexation
import argparse

LANGS = ["en", "fr"]
parser = argparse.ArgumentParser()
parser.add_argument("app_name")


if __name__ == "__main__":
    args = parser.parse_args()
    app_name = args.app_name
    print(f"Creating {app_name}")
    print("Initialize DB")
    import_rules_from_csv()
    import_references_from_csv()
    #tmp
    print("Populate db")
    init_data()

    print("Initialize indexation")
    init_indexation()
    print(f"Generate new app into {app_name}")
    back_app_name = "v3"
    back_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), app_name)
    try:
        shutil.rmtree(back_dir)
    except FileNotFoundError:
        pass
    generate_app(app_name)
