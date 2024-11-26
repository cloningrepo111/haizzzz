import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles

from src.db import check_connection
from src.routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    await check_connection()
    yield
    # shutdown


app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)
app.mount('/static', StaticFiles(directory='static'), name='static')

origins = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(router)


@app.get('/docs', include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + ' - Swagger UI',
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url='/static/swagger-ui-bundle.js',
        swagger_css_url='/static/swagger-ui.css',
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get('/redoc', include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + ' - ReDoc',
        redoc_js_url='/static/redoc.standalone.js',
    )


@app.get('/api/healthchecker')
def root():
    return {'message': 'Welcome to QN\'s Search Service'}


@app.get("/api/get_routes")
def get_routes():
    service = "qn search"
    return {
        "data": {
            "name": "qn search",
            "routes": [
                {"path": route.path, "methods": list(route.methods)[0]}
                for route in app.routes if service in str(route.path).split("/")
            ]
        }
    }


if __name__ == '__main__':
    uvicorn.run("main:app", host='127.0.0.1', port=int(os.getenv("LISTEN_PORT")), reload=True)
    print("Stopping...")
