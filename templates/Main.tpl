#!/usr/bin/env python3.9

from fastapi import FastAPI
from pymongo.mongo_client import MongoClient
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse


{%for model_name in routers%}
from apps.{{model_name}}.routers import router as {{model_name}}_router
{%endfor%}
app = FastAPI()

origins = [
    "http://localhost:8000",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_db_client():
    app.mongodb_client = AsyncIOMotorClient("mongodb://localhost:27017")
    app.mongodb = app.mongodb_client["GD4H_V2"]
    
@app.on_event("shutdown")
async def shutdown_db_client():
    app.mongodb_client.close()


{%for model_name in routers%}
app.include_router({{model_name}}_router, tags=["{{model_name}}s"], prefix="/{{model_name}}")
{%endfor%}


@app.get("/")
async def root():
    response = RedirectResponse(url='/docs')
    return response