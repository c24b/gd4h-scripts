#!/usr/bin/.venv/python3

from __future__ import annotations
from typing import Any, List, Optional
from pydantic import BaseModel, HttpUrl, EmailStr
from enum import Enum
from typing import Optional
from pydantic import BaseModel
from datetime import datetime
{%if import_list is not none%}
{%for import_line in import_list%}
{{import_line}}{%endfor%}
{%endif%}
{% for model_name,value_list in model_dict.items() %}
class {{model_name}}(BaseModel):
    {%for value in value_list %}{{value}}
    {%endfor%}
{%endfor%}

{%if filter_dict is not none%}{% for model_name,value_list in filter_dict.items() %}{%if value_list|length >0%}
class {{model_name}}(BaseModel):
    {%for value in value_list %}{{value}}
    {%endfor%}
{%endif%}{%endfor%}{%endif%}

{%if update_list is not none%}{%for import_line in update_list%}
{{import_line}}
{%endfor%}{%endif%}