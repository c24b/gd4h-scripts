#!/usr/bin/env python3.9
# file: generate_ref_models

from mimetypes import init
import os
import argparse
import shutil
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pathlib import Path
from utils import DB
from utils import data_dir
from utils import AVAILABLE_LANG
from utils import get_rules, is_facet_model, is_multilang_model, is_search_model

curr_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(curr_dir)
# data_dir = os.path.join(parent_dir, "data")
tpl_dir = os.path.join(data_dir, "templates")
app_dir = os.path.join(parent_dir, "new_back")
apps_dir = os.path.join(app_dir, "apps")

env = Environment(loader=FileSystemLoader(tpl_dir), autoescape=select_autoescape())


def get_pydantic_datatype(datatype):
    """transform rules datatype into pydantic datatype"""
    if datatype == "string":
        return "str"
    if datatype == "id":
        return "str"
    if datatype == "boolean":
        return "bool"
    if datatype == "date":
        return "datetime"
    if datatype == "url":
        return "HttpUrl"
    if datatype == "email":
        return "EmailStr"
    if datatype == "object":
        return "dict"


# Generate ModelName
def get_model_name_list():
    """
    build a list of model with [(model_name, lang, ModelName),...]
    """
    models = [("rule", "", "Rules")]
    for model_name in DB["rules"].distinct("model"):
        modelName = model_name.title()
        if is_multilang_model(model_name):
            for lang in AVAILABLE_LANG:
                if is_facet_model(model_name) and is_search_model(model_name):
                    models.append(
                        (model_name, lang, "Filter" + modelName + lang.title())
                    )
                models.append((model_name, lang, modelName + lang.title()))
        else:
            lang = None
            if is_facet_model(model_name):
                models.append((model_name, lang, "Filter" + modelName))
            models.append((model_name, lang, modelName))
    return models


def build_model_with_filter(model_name):
    """build a list of model with [(model_name, lang, Filter<ModelName>),...]"""
    models = []
    modelName = model_name.title()
    if is_multilang_model(model_name):
        for lang in AVAILABLE_LANG:
            if is_facet_model(model_name):
                models.append((model_name, lang, "Filter" + modelName + lang.title()))
            models.append((model_name, lang, modelName + lang.title()))
    else:
        if is_facet_model(model_name):
            models.append((model_name, None, "Filter" + modelName))
        models.append((model_name, None, modelName))
    return models


def build_reference_model(model_name="reference"):
    """build ReferenceModel"""
    models = {}
    for ref_name in DB["references"].distinct("slug"):
        for lang in AVAILABLE_LANG:
            modelName = ref_name.title() + lang.title()
            models[modelName] = []
            for i, value in enumerate(DB["ref_" + ref_name].distinct(f"name_{lang}")):
                models[modelName].append(f'option_{i}="{value}"')
            if len(models[modelName]) == 0:
                del models[modelName]
    models["References"] = ['model: str="reference"',"table_name: str", "slug: str", "refs: List[Reference]"]
    models["Reference"] = ['model: str="reference"',"table_name: str", "ref_slug: str", "name_en: str","name_fr: str", "uri: str", "slug:Optional[str]" ]
    return models


def build_rule_model(model_name="rule"):
    """create a model Rule"""
    models = {}
    modelName = model_name.title()
    models[modelName] = []
    example = DB.rules.find_one()
    for key, value in example.items():
        if key == "_id":
            line = f"{key}: Optional[str]"
        elif key == "external_model_display_keys":
            line = f"{key}: List[str] = []"
        else:
            py_type = type(value).__name__
            if "_en" in key:
                line = f"{key}: Optional[str] = None"
            else:
                line = f"{key}: {py_type} = None"
        models[modelName].append(line)
    return models


def build_imports(model_name):
    """Build import"""
    imports = []
    rules = get_rules(model_name)
    for rule in rules:
        if rule["external_model"] != "":
            external_model_name = rule["external_model"]
            if external_model_name != model_name:
                external_modelName = external_model_name.title()
                
                if is_multilang_model(external_model_name):
                    for lang in AVAILABLE_LANG:
                        imports.append(
                            (external_model_name, lang, external_modelName + lang.title())
                        )
                else:
                    imports.append((external_model_name, "", external_modelName))
    return list(set(imports))


def build_references(model_name):
    """Build references"""
    references = []
    rules = get_rules(model_name)
    for rule in rules:
        if rule["reference_table"] != "":
            if rule["is_controled"]:
                ref_name = rule["reference_table"].replace("ref_", "")
                for lang in AVAILABLE_LANG:
                    references.append(
                        ("reference", lang, ref_name.title() + lang.title())
                    )
    return list(set(references))


def generate_import_lines(model_name, previous_models):
    import_lines = []
    previous_import = [build_imports(m) for m in previous_models]
    previous_imports = [item for sublist in previous_import for item in sublist]
    curr_imports = [
        tag for tag in build_imports(model_name) if tag[0] not in previous_imports and tag[-1] != "Reference"
    ]
    models_and_references_list = curr_imports + build_references(model_name)
    # print(models_and_references_list)
    for ext_model_name, lang, modelName in models_and_references_list:
        import_lines.append(f"from apps.{ext_model_name}.models import {modelName}")
    if len(import_lines) > 0:
        return list(set(import_lines))
    return None


def generate_update_lines(model_name, previous_models):
    import_lines = []
    previous_import = [build_imports(m) for m in previous_models]
    previous_imports = list(
        set([item[0] for sublist in previous_import for item in sublist])
    )
    curr_imports = [
        tag for tag in build_imports(model_name) if tag[0] not in previous_imports
    ]
    for ext_model_name, lang, modelName in curr_imports:
        if ext_model_name != "reference" and ext_model_name != model_name:
            import_lines.append(f"{modelName}.update_forward_refs()")
    if len(import_lines) > 0:
        return list(set(import_lines))
    return None


def generate_model_lines(model_name):
    lines = []
    for model_name, lang, ModelName in build_model_with_filter(model_name):
        # if ModelName != "Reference":
        lines.append(f"from .models import {ModelName}")
    if len(lines) > 0:
        return lines
    return None


def generate_services_lines(model_name):
    return [
        "from .services import es, index_document, get_indexed_fieldnames, search_documents, sync_get_filters"
    ]


def get_reference_model(ref_model_name, lang):
    """Return the corresponding reference"""
    if lang is not None:
        return ref_model_name.replace("ref_", "").title() + lang.title()
    else:
        #if lang is None => default to en
        lang = "en"
        return ref_model_name.replace("ref_", "").title() + lang.title()


def get_external_model(external_model_name, lang):
    """Return the corresponding reference"""
    if is_multilang_model(external_model_name):
        if lang is None:
            #if lang is None => default to en
            lang = "en"
        return external_model_name.title() + lang.title()
    else:
        return external_model_name.title()


def get_filter_rules(model_name):
    return DB.rules.find({"model": model_name, "is_facet": True}, {"_id": 0})


def build_filter_model_properties(model_name, lang):
    if lang is not None:
        modelName = "Filter" + model_name.title() + lang.title()
        # model_lines[modelName] = []
    else:
        modelName = "Filter" + model_name.title()
    model_lines = []
    for rule in get_filter_rules(model_name):
        field_name = rule["slug"]
        py_type = get_pydantic_datatype(rule["datatype"])
        if rule["multiple"]:
            if rule["reference_table"] != "":
                if rule["is_controled"]:
                    py_type = get_reference_model(rule["reference_table"], lang)
                else:
                    py_type = "str"
            elif rule["external_model"] != "":
                py_type = get_external_model(rule["external_model"], lang)

            if rule["mandatory"]:
                line = f"{field_name}: List[{py_type}] = []"
            else:
                line = f"{field_name}: Optional[List[{py_type}]] = []"
            # line = f"{field_name}: List[{py_type}] = []"
            model_lines.append(line)
            continue
        else:
            if rule["reference_table"] != "":
                if rule["is_controled"]:
                    py_type = get_reference_model(rule["reference_table"], lang)
                else:
                    py_type = "str"

            elif rule["external_model"] != "":
                py_type = get_external_model(rule["external_model"], lang)

            if rule["mandatory"]:
                line = f"{field_name}: {py_type}"
            else:
                line = f"{field_name}: Optional[{py_type}] = None"
            model_lines.append(line)
            continue
    return {modelName: model_lines}


def build_model_properties(model_name, lang):
    if lang is not None:
        modelName = model_name.title() + lang.title()
        # model_lines[modelName] = []
    else:
        modelName = model_name.title()
    model_lines = []
    for rule in get_rules(model_name):
        field_name = rule["slug"]
        py_type = get_pydantic_datatype(rule["datatype"])
        if rule["multiple"]:
            if rule["reference_table"] != "":
                if rule["is_controled"]:
                    py_type = get_reference_model(rule["reference_table"], lang)
                else:
                    py_type = "str"
            elif rule["external_model"] != "":
                py_type = get_external_model(rule["external_model"], lang)
                
            if rule["mandatory"]:
                line = f"{field_name}: List[{py_type}] = []"
            else:
                line = f"{field_name}: Optional[List[{py_type}]] = []"
            # line = f"{field_name}: List[{py_type}] = []"
            
            model_lines.append(line)
            continue
        else:
            if rule["reference_table"] != "":
                if rule["is_controled"]:
                    py_type = get_reference_model(rule["reference_table"], lang)
                else:
                    py_type = "str"

            elif rule["external_model"] != "":
                py_type = get_external_model(rule["external_model"], lang)
                
            if rule["mandatory"]:
                line = f"{field_name}: {py_type}"
            else:
                line = f"{field_name}: Optional[{py_type}] = None"
            model_lines.append(line)
            continue
    return {modelName: model_lines}


def build_model(model_name):
    """build FastApi model from model_name"""
    model_lines = {}
    if is_multilang_model(model_name):
        for lang in AVAILABLE_LANG:
            model_lines.update(build_model_properties(model_name, lang))
    else:
        model_lines.update(build_model_properties(model_name, None))
    return model_lines


def build_filter_model(model_name):
    """build FastApi filter model from model_name"""
    model_lines = {}
    if is_multilang_model(model_name):
        for lang in AVAILABLE_LANG:
            model_lines.update(build_filter_model_properties(model_name, lang))
    else:
        model_lines.update(build_filter_model_properties(model_name, None))
    return model_lines


def generate_model(
    model_name, output_file, template_file="Model.tpl", previous_models=[]
):
    """Generate model from model_name to models.py"""
    template = env.get_template(template_file)
    import_list = generate_import_lines(model_name, previous_models)
    if model_name == "reference":
        model_dict = build_reference_model(model_name)
    elif model_name == "rule":
        model_dict = build_rule_model(model_name)
    else:
        model_dict = build_model(model_name)
    filter_dict = build_filter_model(model_name)
    update_list = generate_update_lines(model_name, previous_models)
    with open(output_file, "w") as f:
        py_file = template.render(
            import_list=import_list,
            model_dict=model_dict,
            filter_dict=filter_dict,
            update_list=update_list,
        )
        f.write(py_file)


def get_models(model_name):
    """List models from model_name: (model_name, lang, ModelName<Lang>)"""
    models = []
    modelName = model_name.title()
    if is_multilang_model(model_name):
        for lang in AVAILABLE_LANG:
            models.append((model_name, lang, modelName + lang.title()))
    else:
        models.append((model_name, None, modelName))
    return models


def get_filters_models(model_name):
    """List models from model_name: (model_name, lang, FilterModelName<Lang>)"""
    models = []
    modelName = model_name.title()
    if is_multilang_model(model_name):
        for lang in AVAILABLE_LANG:
            if is_facet_model(model_name) and is_search_model(model_name):
                models.append((model_name, lang, "Filter" + modelName + lang.title()))
            # models.append((model_name, lang, modelName + lang.title()))
    else:
        if is_facet_model(model_name) and is_search_model(model_name):
            models.append((model_name, lang, "Filter" + modelName))
        # models.append((model_name, lang, modelName))
    if len(models) > 0:
        return models


def generate_router(model_name, output_file):
    """Write routers.py file from model_name"""
    models = get_models(model_name)
    filters = get_filters_models(model_name)
    template = env.get_template("Router.tpl")
    import_models = generate_model_lines(model_name)
    import_services = generate_services_lines(model_name)
    translate = is_multilang_model(model_name)
    search = is_search_model(model_name)
    filter = is_facet_model(model_name)
    LANGS = AVAILABLE_LANG
    with open(output_file, "w") as f:
        py_file = template.render(
            model_name=model_name,
            models=models,
            import_models=import_models,
            import_services=import_services,
            filters=filters,
            translate=translate,
            search=search,
            filter=filter,
            LANGS=LANGS,
        )
        f.write(py_file)


def generate_services(model_name, output_file):
    """Write services.py file from model_name"""
    template = env.get_template("services.tpl")
    with open(output_file, "w") as f:
        py_file = template.render(model_name=model_name)
        f.write(py_file)


def generate_main(output_file):
    """Write main.py"""
    template = env.get_template("Main.tpl")
    with open(output_file, "w") as f:
        py_file = template.render(
            routers=set([n[0] for n in get_model_name_list()]), langs=AVAILABLE_LANG
        )
        f.write(py_file)


def generate_endpoint(apps_dir, model_name, previous_models):
    """Generate a new endpoint <model_name>/ with models.py routers.py and services.py"""
    # create apps.model_name dir
    model_dir = os.path.join(apps_dir, model_name)
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
        init_file = os.path.join(model_dir, "__init__.py")
        Path(init_file).touch()
    model_file = os.path.join(model_dir, "models.py")
    generate_model(model_name, model_file, "Model.tpl", previous_models)
    router_file = os.path.join(model_dir, "routers.py")
    generate_router(model_name, router_file)
    # if is_search_model(model_name) or is_facet_model(model_name):
    service_file = os.path.join(model_dir, "services.py")
    generate_services(model_name, service_file)
    return


def generate_app(back_name="test_back"):
    back_dir = os.path.join(parent_dir, back_name)
    apps_dir = os.path.join(back_dir, "apps")
    if not os.path.exists(back_dir):
        os.makedirs(back_dir)
    if not os.path.exists(apps_dir):
        os.makedirs(apps_dir)
    print("Creating app")
    model_list = DB.rules.distinct("model") + ["rule"]
    for i, model_name in enumerate(model_list):
        if i == 0:
            generate_endpoint(apps_dir, model_name, previous_models=[])
        else:
            generate_endpoint(
                apps_dir, model_name, previous_models=model_list[0 : i - 1]
            )
    main_file = os.path.join(back_dir, "main.py")
    generate_main(main_file)
    init_file = os.path.join(back_dir, "__init__.py")
    Path(init_file).touch()
    print(f"sucessfully generated app in {back_dir}")
    print(f"Run\ncd {back_dir}\nuvicorn main:app --reload")


parser = argparse.ArgumentParser()
parser.add_argument("app_name")


if __name__ == "__main__":
    args = parser.parse_args()
    app_name = args.app_name
    print(f"Creating {app_name}")
    print(f"Generate new app into {app_name}")
    # # back_app_name = "new_back"
    back_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), app_name)
    try:
        shutil.rmtree(back_dir)
    except FileNotFoundError:
        pass
    generate_app(app_name)
