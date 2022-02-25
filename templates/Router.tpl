#!/usr/bin/.env/python3.9
# file: routers.py

import json
from fastapi import APIRouter, Body, Request, HTTPException, status, Query
from fastapi.responses import JSONResponse
from bson import json_util, ObjectId
from typing import Optional

{%if import_models is not none %}{%for import_line in import_models %}
{{import_line}}{%endfor%}
{%endif%}

{%if import_services is not none %}{% for service_name in import_services %}
{{service_name}}{% endfor %}
{%endif%}
router = APIRouter()

def parse_json(data):
    return json.loads(json_util.dumps(data))

@router.get("/", response_description="Get all {{model_name}}s")
async def get_{{model_name}}s(request: Request, lang:str = "fr"):
    {{model_name}}s = []
    for doc in await request.app.mongodb["{{model_name}}s"].find({}).to_list(length=120):
        doc_id = doc["_id"]
        doc = doc[lang]
        doc["_id"] = str(doc_id)
        {{model_name}}s.append(doc)
    if len({{model_name}}s) == 0:
        raise HTTPException(status_code=404, detail=f"No {{model_name}}s found")
    return parse_json({{model_name}}s)

@router.delete("/{item_id}", response_description="Delete a {{model_name}}")
async def update_{{model_name}}(request:Request, item_id: str):
    del_{{model_name}} = await request.app.mongodb["{{model_name}}s"].delete_one({"_id": ObjectId(item_id)})
    {% if search %}{% for lang in LANGS %}delete_document(item_id, model="{{model_name}}", lang="{{lang}}")
    {% endfor %}{% endif %}
    return JSONResponse(status_code=status.HTTP_204_DELETED)

{%for model_name, lang, modelName in models%}
{%if lang is not none %}
@router.post("?lang={{lang}}", response_description="Add a {{model_name}}")
async def create_{{model_name}}_{{lang}}(request:Request, {{model_name}}:{{modelName}} = Body(...), lang:str="{{lang}}"):
    {{model_name}} = parse_json({{model_name}})
    {%if translate%}
    stored_{{model_name}} = {"en":{}, "fr":{}}
    stored_{{model_name}}[lang] = {{model_name}}
    #here translate
    #other_lang = SWITCH_LANGS(lang)
    #stored_{{model_name}}[other_lang] = translate_doc(organization, _from={{lang}})
    {%else %}
    stored_{{model_name}} = {{model_name}}
    {%endif%}
    new_{{model_name}} = await request.app.mongodb["{{model_name}}s"].insert_one(stored_{{model_name}})
    created_{{model_name}} = await request.app.mongodb["{{model_name}}s"].find_one({"_id": new_{{model_name}}.inserted_id})
    #here index document at insert
    {% if index %}    
    #here index
    index_document("{{model_name}}", created_{{model_name}})
    index_document(stored_{{model_name}},"{{model_name}}" , lang)
    {%endif%}
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=created_{{model_name}})
{%else %}
@router.post("", response_description="Add a {{model_name}}")
async def create_{{model_name}}(request:Request, {{model_name}}:{{modelName}} = Body(...)):
    {{model_name}} = parse_json({{model_name}})
    stored_{{model_name}} = {{model_name}}
    new_{{model_name}} = await request.app.mongodb["{{model_name}}s"].insert_one(stored_{{model_name}})
    created_{{model_name}} = await request.app.mongodb["{{model_name}}s"].find_one({"_id": new_{{model_name}}.inserted_id})
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=created_{{model_name}})
{%endif%}{%endfor%}

{%for model_name, lang, modelName in models%}
{%if lang is not none %}
@router.put("/{item_id}?lang={{lang}}", response_description="Update a {{model_name}}")
async def update_{{model_name}}_{{lang}}(request:Request, item_id: str, {{model_name}}: {{modelName}} = Body(...), lang:str="{{lang}}"):
    {{model_name}} = parse_json({{model_name}})
    {%if translate%}
    stored_{{model_name}} = {"en":{}, "fr":{}}
    stored_{{model_name}}[lang] = {{model_name}}
    #here translate
    #other_lang = SWITCH_LANGS(lang)
    #stored_{{model}}[other_lang] = translate_doc(organization, _from=lang)
    {%else %}
    stored_{{model_name}} = {{model_name}}
    {%endif%}
    # created_{{model_name}} = await request.app.mongodb["{{model_name}}s"].find_one({"_id": updated_{{model_name}}.inserted_id})
    # {%if index%}
    # index_document("{{model_name}}", created_{{model_name}})
    # index_document(stored_{{model_name}},"{{model_name}}" , lang)
    {%endif%}
    return JSONResponse(status_code=status.HTTP_204_UPDATED, content=created_{{model_name}})
{%else%}
@router.put("/{item_id}", response_description="Update a {{model_name}}")
async def update_{{model_name}}(request:Request, item_id: str, {{model_name}}: {{modelName}} = Body(...)):
    {{model_name}} = parse_json({{model_name}})
    stored_{{model_name}} = {{model_name}}
    return JSONResponse(status_code=status.HTTP_204_UPDATED, content=stored_{{model_name}})
{%endif%}
{%endfor%}
{% if search %}
{%for model_name, lang, modelName in models%}
@router.get("/search?lang={{lang}}", response_description="Search for {{model_name}}s using full_text_query")
async def search_{{model_name}}s_{{lang}}(request:Request, query: Optional[str] = Query(None, min_length=2, max_length=50), lang:str="{{lang}}"):
    fields = get_indexed_fieldnames(model="{{model_name}}")
    final_query = {
        "multi_match" : {
        "query":    query.strip(), 
        "fields": fields
        }
    }
    highlight = {
        "pre_tags" : "<em class='tag-fr highlight'>",
        "post_tags" :"</em>",
        "fields" : {f:{} for f in fields }
        }
    results = search_documents(final_query, highlight, model="{{model_name}}", lang=lang)
    results["query"] = query
    return results
{%endfor %}
{%endif %}
{%if filter %}
@router.get("/filters")
async def get_filters(request:Request, lang: str="fr"):
    filters = sync_get_filters(lang)
    return parse_json(filters)
{%if filters is not none %}{%for model_name, lang, modelName in filters%}
@router.post("/filters?lang={{lang}}")
async def filter_{{model_name}}s(request:Request, filter:{{modelName}}, lang:str="{{lang}}"):
    req_filter = await request.json()
    index_name = f"{{model_name}}_{{lang}}"
    print(req_filter)
    if len(req_filter) == 1:
        param_k = list(req_filter.keys())[0]
        param_v = list(req_filter.values())[0]
        if param_k == "organizations":
            final_q = {
                "nested": {
                    "path": param_k,
                    "query": {
                    "match": {
                        f"{param_k}.name": ",".join(param_v)
                       }
                    }
                }
            }
            
        else:
            if isinstance(param_v, list):
                final_q = {"match": {param_k: {"query": ",".join(param_v)}}}
            else:
                final_q = {"match": {param_k: {"query": param_v}}}
        # highlight = {}
        # results = search_documents(final_q, highlight,model="{{model_name}}", lang=lang)
        print(final_q)
    else:
        must = []
        for key, val in req_filter.items():
            if key == "organizations":
                nested_q = {"nested": {
                    "path": key,
                    "query": {
                    "match": {
                        f"{key}.name": ",".join(val)
                       }
                    }
                }}
                must.append(nested_q)
            else:
                if isinstance(val, list):
                    must.append({"match":{key:",".join(val)}})
                else:
                    must.append({"match":{key:val}})
        final_q = {"bool" : { "must":must}}
        print(final_q)
    highlight = {}
    results = search_documents(final_q, highlight, model="{{model_name}}", lang=lang)
    results["query"] = req_filter
    print(results)
    return results
{%endfor%}
{%endif%}{%endif%}

{%for model_name, lang, modelName in models%}
{%if lang is not none %}
@router.get("/{item_id}?lang={{lang}}", response_description="Get one {{model_name}}")
async def get_{{model_name}}_{{lang}}(request: Request, item_id: str, lang:str = "{{lang}}"):
    if ({{model_name}} := await request.app.mongodb["{{model_name}}s"].find_one({"_id": ObjectId(item_id)})) is not None:
        doc_id = {{model_name}}["_id"]
        doc = {{model_name}}[lang]
        doc["_id"] = str(doc_id)
        return parse_json(doc)
    raise HTTPException(status_code=404, detail=f"{{modelName}} {item_id} not found")
{%else%}
@router.get("/{item_id}", response_description="Get one {{model_name}}")
async def get_{{model_name}}(request: Request, item_id: str):
    if ({{model_name}} := await request.app.mongodb["{{model_name}}s"].find_one({"_id": ObjectId(item_id)})) is not None:
        doc_id = {{model_name}}["_id"]
        doc["_id"] = str(doc_id)
        return parse_json(doc)
    raise HTTPException(status_code=404, detail=f"{{modelName}} {item_id} not found")
{%endif%}
{%endfor%}