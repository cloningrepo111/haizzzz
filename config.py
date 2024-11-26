import os
from typing import Any
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()


class Settings(BaseModel):
    # MongoDB
    MONGO_DATABASE_URL: str | None = None
    MONGO_INITDB_DATABASE: str | None = None
    MONGO_INITDB_ROOT_USERNAME: str | None = None
    MONGO_INITDB_ROOT_PASSWORD: str | None = None

    ELASTIC_URL: str | None = None
    ELASTIC_USR: str | None = None
    ELASTIC_PWD: str | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        data = dict(os.environ)
        if data:
            self.MONGO_DATABASE_URL = data.get("MONGO_DATABASE_URL")
            self.MONGO_INITDB_DATABASE = data.get("MONGO_INITDB_DATABASE")
            self.MONGO_INITDB_ROOT_USERNAME = data.get("MONGO_INITDB_ROOT_USERNAME")
            self.MONGO_INITDB_ROOT_PASSWORD = data.get("MONGO_INITDB_ROOT_PASSWORD")

            self.ELASTIC_URL = data.get("ELASTIC_URL")
            self.ELASTIC_USR = data.get("ELASTIC_USR")
            self.ELASTIC_PWD = data.get("ELASTIC_PWD")


settings = Settings()
