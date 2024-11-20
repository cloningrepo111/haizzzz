from bson.objectid import ObjectId

from src.db import Role, User, Resource
from src.models.response_schema import ResponseModel
# from src.serializers.search_serializer import user_entity


# async def check_user_role(user_id: str, system_name: str, endpoint_privileges: list[str]) -> ResponseModel:
#     try:
#         user = await User.find_one({"_id": ObjectId(user_id)})
#         if user:
#             check_role = await Role.find_one({
#                 "name": user.get("role"),
#                 "system_privileges": {
#                     "$elemMatch": {
#                         "name": {"$in": ["all", system_name]},
#                         "privileges": {"$in": endpoint_privileges}
#                     }
#                 }
#             })
#             if check_role:
#                 return ResponseModel(status=True,
#                                      data=user_entity(user),
#                                      message="Verify role name successfully")
#             else:
#                 return ResponseModel(status=False,
#                                      data=None,
#                                      message="Don't have privileges to perform this action")
#         else:
#             return ResponseModel(status=False,
#                                  data=None,
#                                  message="No matches user found")
#     except Exception as e:
#         print(f"Unable to verify user, {e}")
#         return ResponseModel(status=False,
#                              data=None,
#                              message="Unable to verify user")


async def get_indexs(role_name: str) -> list:
    pipeline = [
        {
            "$match": {
                "name": role_name
            }
        },
        {
            "$unwind": "$resource_privileges"
        },
        {
            "$replaceRoot": {
                "newRoot": "$resource_privileges"
            }
        },
        {
            "$unwind": "$privileges"
        },
        {
            "$group": {
                "_id": "$privileges",
                "names": {
                    "$push": "$name"
                }
            }
        },
        {
            "$match": {
                "_id": "read"
            }
        },
        {
            "$lookup": {
                "from": "resources",
                "localField": "names",
                "foreignField": "name",
                "pipeline": [
                    {
                        "$unwind": "$tenant"
                    }
                ],
                "as": "result"
            }
        },
        {
            "$unwind": "$result"
        },
        {
            "$group": {
                "_id": None,
                "indexs": {
                    "$push": "$result.tenant"
                }
            }
        }
    ]
    indexs = await Role.aggregate(pipeline=pipeline).to_list(length=1)
    if len(indexs) > 0:
        return list(set(indexs[0].get("indexs")))
    else:
        return indexs


async def get_indexs_by_system_admin() -> list:
    pipeline = [
        {
            "$unwind": "$tenant"
        },
        {
            "$group": {
                "_id": None,
                "indexs": {
                    "$push": "$tenant"
                }
            }
        }
    ]
    indexs = await Resource.aggregate(pipeline=pipeline).to_list(length=1)
    if len(indexs) > 0:
        return list(set(indexs[0].get("indexs")))
    else:
        return indexs


def get_tenants(indexs) -> list:
    tenants = set()
    for index in indexs:
        sub = str(index).split("-")
        tenants.add(sub[0])
    return list(tenants)


async def get_resources_by_role(role_name: str) -> list:
    pipeline = [
        {
            "$match": {
                "name": role_name
            }
        },
        {
            "$unwind": "$resource_privileges"
        },
        {
            "$unwind": "$resource_privileges.privileges"
        },
        {
            "$match": {
                "resource_privileges.privileges": "read"
            }
        },
        {
            "$group": {
                "_id": "$name",
                "resources": {
                    "$push": "$resource_privileges.name"
                }
            }
        }
    ]
    resources = await Role.aggregate(pipeline=pipeline).to_list(length=1)
    if len(resources) > 0:
        return list(set(resources[0].get("resources")))
    else:
        return resources


async def get_resources_by_system_admin() -> list:
    pipeline = [
        {
            "$group": {
                "_id": None,
                "resources": {
                    "$push": "$name"
                }
            }
        }
    ]
    resources = await Resource.aggregate(pipeline=pipeline).to_list(length=1)
    if len(resources) > 0:
        return list(set(resources[0].get("resources")))
    else:
        return resources


async def get_indexs_by_resource(resources: list[str]) -> list:
    pipeline = [
        {
            "$match": {
                "name": {"$in": resources}
            }
        },
        {
            "$unwind": "$tenant"
        },
        {
            "$group": {
                "_id": None,
                "indexs": {
                    "$push": "$tenant"
                }
            }
        }
    ]
    indexs = await Resource.aggregate(pipeline=pipeline).to_list(length=1)
    if len(indexs) > 0:
        return list(set(indexs[0].get("indexs")))
    else:
        return indexs
