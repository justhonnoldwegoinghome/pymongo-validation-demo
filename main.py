from typing import Optional
from typing import Any
from pydantic import BaseModel
from pydantic import PositiveInt
from pymongo import MongoClient
from pymongo.errors import WriteError

DB_SERVER_URI = "mongodb://localhost:27017"

client: MongoClient = MongoClient(DB_SERVER_URI, tz_aware=True)
db = client.get_database("demoDB")
users_collection = db.users
db.command(
    "collMod",
    "users",
    validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id", "name", "age"],
            "properties": {
                "id": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "name": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "age": {
                    "bsonType": "int",
                    "minimum": 21,
                    "description": "must be an integer >= 0 and is required",
                },
            },
        }
    },
)


class User(BaseModel):
    id: str
    name: str
    age: PositiveInt


def create_user(u: User) -> Optional[User]:
    try:
        users_collection.insert_one(u.model_dump())
        user: dict[str, Any] | None = users_collection.find_one(
            {"id": u.id}, {"_id": 0, "id": 1, "name": 1, "age": 1}
        )
        if user:
            return User(**user)  # Pydantic will raise ValueError if cmi
        else:
            return None
    except WriteError:
        print("Invalid user")
        return


if __name__ == "__main__":
    valid_user = User(id="1", name="Jeff", age=30)
    create_user(valid_user)

    invalid_user = User(id="2", name="Boots", age=18)
    create_user(invalid_user)
