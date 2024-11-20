import re
from typing import Any

from src.db import Alias
from src.models.new_query_schema import Filter
from src.models.response_schema import ResponseModel
from src.utils.alias_generate import generate_range_ip_alias, \
    generate_range_port_alias, generate_subnet_alias

alias_patterns = r"^(alias\.(ip|port|mac|domain|name|signature|subnet|range_ip|range_port)\.\w+)$"
nested_key_patterns = r"^(\w+(\.\w+)+)$"

mapping_types = {
    "number": ["long", "integer", "short", "byte",
               "double", "float", "half_float",
               "scaled_float", "unsigned_long"],
    "date": ["date"],
    "keyword": ["keyword"],
    "text": ["text"],
    "object": ["nested", "flattened"],
    "ip": ["ip"],
    "boolean": ["boolean"]
}


def validate_field_for_query(fields: list[str], properties: dict) -> ResponseModel:
    for field in fields:
        if properties.get(field) is None:
            if re.fullmatch(nested_key_patterns, field) is not None:
                sub = field.split(".")
                key = ".".join(sub[:len(sub) - 1])
                sub_type = sub[len(sub) - 1]
                if properties.get(key):
                    if properties[key].get("type") in mapping_types["object"]:
                        pass
                    else:
                        if sub_type not in properties[key].get("sub_types"):
                            return ResponseModel(status=False,
                                                 data=None,
                                                 message="Failure! Unable to validate the query due to an invalid key field.")
                else:
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! Unable to validate the query due to an invalid key field.")
            else:
                return ResponseModel(status=False,
                                     data=None,
                                     message="Failure! Unable to validate the query due to an invalid key field.")
    return ResponseModel(status=True,
                         data=None,
                         message="Done")


def validate_field_by(fields: list[str], properties: dict, defined_types: list[str]) -> ResponseModel:
    for field in fields:
        if properties.get(field) is None:
            if re.fullmatch(nested_key_patterns, field) is not None:
                sub = field.split(".")
                key = ".".join(sub[:len(sub) - 1])
                sub_type = sub[len(sub) - 1]
                if properties.get(key):
                    if sub_type not in properties[key].get("sub_types"):
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Failure! Unable to validate the query due to an invalid key field.")
                    if not any(sub_type in mapping_types[defined_type] for defined_type in defined_types):
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Failure! Unable to validate the query due to an invalid key field.")
                else:
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! Unable to validate the query due to an invalid key field.")
            else:
                return ResponseModel(status=False,
                                     data=None,
                                     message="Failure! Unable to validate the query due to an invalid key field.")
        else:
            if not any(properties[field].get("type") in mapping_types[defined_type] for defined_type in defined_types):
                return ResponseModel(status=False,
                                     data=None,
                                     message="Failure! Unable to validate the query due to an invalid key field.")

    return ResponseModel(status=True,
                         data=None,
                         message="Done")


async def decode_value_from_alias(value: Any, user_id: str, role_name: str) -> ResponseModel:
    try:
        if isinstance(value, str) and re.fullmatch(alias_patterns, value) is not None:
            sub_value = value.split(".")
            if role_name == "system-admin":
                alias = await Alias.find_one({
                    "name": sub_value[2],
                    "type": sub_value[1]
                })
            else:
                alias = await Alias.find_one({
                    "$and": [
                        {"name": sub_value[2]},
                        {"type": sub_value[1]},
                        {
                            "$or": [
                                {"tag": "system"},
                                {"owners": {"$in": [user_id]}},
                                {
                                    "permissions": {
                                        "$elemMatch": {
                                            "name": role_name,
                                            "privileges": {"$in": ["read"]}
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                })

            if alias:
                result = []
                match alias.get("type"):
                    case "subnet":
                        for data in alias.get("data"):
                            if re.fullmatch(alias_patterns, data.get("value")):
                                alias_in_alias = await decode_value_from_alias(
                                    data.get("value"),
                                    user_id,
                                    role_name
                                )
                                if not alias_in_alias.status:
                                    return alias_in_alias

                                if isinstance(alias_in_alias.data, list):
                                    result += alias_in_alias.data
                                else:
                                    result.append(alias_in_alias.data)
                            else:
                                subnet = generate_subnet_alias(data.get("value"))
                                if subnet:
                                    result += subnet
                    case "range_ip":
                        for data in alias.get("data"):
                            if re.fullmatch(alias_patterns, data.get("value")):
                                alias_in_alias = await decode_value_from_alias(
                                    data.get("value"),
                                    user_id,
                                    role_name
                                )
                                if not alias_in_alias.status:
                                    return alias_in_alias

                                if isinstance(alias_in_alias.data, list):
                                    result += alias_in_alias.data
                                else:
                                    result.append(alias_in_alias.data)
                            else:
                                range_ip = generate_range_ip_alias(data.get("value"))
                                if range_ip:
                                    result += range_ip
                    case "range_port":
                        for data in alias.get("data"):
                            if re.fullmatch(alias_patterns, data.get("value")):
                                alias_in_alias = await decode_value_from_alias(
                                    data.get("value"),
                                    user_id,
                                    role_name
                                )
                                if not alias_in_alias.status:
                                    return alias_in_alias

                                if isinstance(alias_in_alias.data, list):
                                    result += alias_in_alias.data
                                else:
                                    result.append(alias_in_alias.data)
                            else:
                                range_port = generate_range_port_alias(data.get("value"))
                                if range_port:
                                    result += range_port
                    case _:
                        for data in alias.get("data"):
                            if isinstance(data.get("value"), str):
                                if re.fullmatch(alias_patterns, data.get("value")):
                                    alias_in_alias = await decode_value_from_alias(
                                        data.get("value"),
                                        user_id,
                                        role_name
                                    )
                                    if not alias_in_alias.status:
                                        return alias_in_alias

                                    if isinstance(alias_in_alias.data, list):
                                        result += alias_in_alias.data
                                    else:
                                        result.append(alias_in_alias.data)
                                else:
                                    result.append(data.get("value"))
                            else:
                                result.append(data.get("value"))

                return ResponseModel(status=True,
                                     data=result,
                                     message="Done")

        return ResponseModel(status=True,
                             data=value,
                             message="Done")
    except Exception as e:
        print(f"Decoding alias values failed, {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! The process decode_alias has encountered an error.")


async def parse_queries(data: list[dict], properties: dict, user_id: str, role_name: str) -> ResponseModel:
    query = []
    for item in data:
        temp_query = {"bool": {"must": []}}
        query.append(temp_query)
        for field in item:
            if properties.get(field) is None:
                if re.fullmatch(nested_key_patterns, field) is not None:
                    sub = str(field).split(".")
                    key = ".".join(sub[:len(sub) - 1])
                    sub_type = sub[len(sub) - 1]
                    if properties.get(key):
                        new_value = await decode_value_from_alias(item[field], user_id, role_name)
                        if not new_value.status:
                            return new_value

                        if properties[key].get("type") in mapping_types["object"]:
                            if isinstance(new_value.data, list):
                                temp_query["bool"]["must"].append({
                                    "bool": {
                                        "should": [{"match_phrase": {field: value}} for value in new_value.data]
                                    }
                                })
                            elif isinstance(new_value.data, dict):
                                for item_key in new_value.data:
                                    if item_key not in ["gt", "lt", "gte", "lte"]:
                                        return ResponseModel(status=False,
                                                             data=None,
                                                             message="Failure! Query data is invalid. Please check your input.")
                                temp_query["bool"]["must"].append({"range": {field: new_value.data}})
                            else:
                                temp_query["bool"]["must"].append({"match_phrase": {field: new_value.data}})
                        else:
                            if sub_type not in properties[key].get("sub_types"):
                                return ResponseModel(status=False,
                                                     data=None,
                                                     message="Failure! Unable to validate the query due to an invalid key field.")

                            if sub_type in mapping_types["text"]:
                                if isinstance(new_value.data, str):
                                    temp_query["bool"]["must"].append({"match_phrase": {field: new_value.data}})
                                elif isinstance(new_value.data, list):
                                    temp_query["bool"]["must"].append({
                                        "bool": {
                                            "should": [{"match_phrase": {field: value}} for value in new_value.data]
                                        }
                                    })
                                else:
                                    return ResponseModel(status=False,
                                                         data=None,
                                                         message="Failure! Query data is invalid. Please check your input.")
                            else:
                                if isinstance(new_value.data, list):
                                    temp_query["bool"]["must"].append({"terms": {field: new_value.data}})
                                elif isinstance(new_value.data, dict):
                                    for item_key in new_value.data:
                                        if item_key not in ["gt", "lt", "gte", "lte"]:
                                            return ResponseModel(status=False,
                                                                 data=None,
                                                                 message="Failure! Query data is invalid. Please check your input.")
                                    temp_query["bool"]["must"].append({"range": {field: new_value.data}})
                                else:
                                    temp_query["bool"]["must"].append({"term": {field: new_value.data}})
                    else:
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Failure! Unable to validate the query due to an invalid key field.")
                else:
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! Unable to validate the query due to an invalid key field.")
            else:
                new_value = await decode_value_from_alias(item[field], user_id, role_name)
                if not new_value.status:
                    return new_value

                if properties[field].get("type") in mapping_types["object"]:
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! Query data is invalid. Please check your input.")

                if properties[field].get("type") in mapping_types["text"]:
                    if isinstance(new_value.data, str):
                        temp_query["bool"]["must"].append({"match_phrase": {field: new_value.data}})
                    elif isinstance(new_value.data, list):
                        temp = {"bool": {"should": []}}
                        temp_query["bool"]["must"].append(temp)
                        for value in new_value.data:
                            if isinstance(value, str):
                                temp["bool"]["should"].append({"match_phrase": {field: value}})
                            else:
                                return ResponseModel(status=False,
                                                     data=None,
                                                     message="Failure! Query data is invalid. Please check your input.")
                    else:
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Failure! Query data is invalid. Please check your input.")
                else:
                    if isinstance(new_value.data, str):
                        temp_query["bool"]["must"].append({"term": {field: new_value.data}})
                    elif isinstance(new_value.data, list):
                        temp_query["bool"]["must"].append({"terms": {field: new_value.data}})
                    elif isinstance(new_value.data, dict):
                        for sub in new_value.data:
                            if sub not in ["gt", "lt", "gte", "lte"]:
                                return ResponseModel(status=False,
                                                     data=None,
                                                     message="Failure! Query data is invalid. Please check your input.")
                        temp_query["bool"]["must"].append({"range": {field: new_value.data}})
                    else:
                        temp_query["bool"]["must"].append({"term": {field: new_value.data}})

    return ResponseModel(status=True,
                         data=query,
                         message="Done")


def selected_query(field: str, properties: dict, is_list: bool):
    result = None
    if properties.get(field) is None:
        if re.fullmatch(nested_key_patterns, field) is not None:
            sub = str(field).split(".")
            key = ".".join(sub[:len(sub) - 1])
            sub_type = sub[len(sub) - 1]
            if properties.get(key):
                if properties[key].get("type") in mapping_types["object"]:
                    result = "match_phrase"
                else:
                    if sub_type in properties[key].get("sub_types"):
                        if sub_type in mapping_types["text"]:
                            result = "match_phrase"
                        elif sub_type not in mapping_types["object"]:
                            result = "terms" if is_list else "term"
    else:
        if properties[field].get("type") in mapping_types["text"]:
            result = "match_phrase"
        elif properties[field].get("type") not in mapping_types["object"]:
            result = "terms" if is_list else "term"

    return result


async def generate_filter(payload: Filter, properties: dict, user_id: str, role_name: str):
    if payload.operator == "and":
        query = {
            "bool": {
                "must": []
            }
        }
        main_query = query["bool"]["must"]
    else:
        query = {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
        main_query = query["bool"]["should"]

    for item in payload.items:
        match item.operator:
            case "is":
                if isinstance(item.value, str):
                    new_value = await decode_value_from_alias(
                        item.value,
                        user_id,
                        role_name
                    )
                    if not new_value.status:
                        return None
                    else:
                        if isinstance(new_value.data, list):
                            key_query = selected_query(item.key, properties, True)
                        else:
                            key_query = selected_query(item.key, properties, False)
                        value = new_value.data
                else:
                    key_query = selected_query(item.key, properties, False)
                    value = item.value

                if key_query:
                    if key_query == "match_phrase" and isinstance(value, list):
                        main_query.append({
                            "bool": {
                                "must": {
                                    "bool": {
                                        "should": [{key_query: {item.key: item_value}} for item_value in value]
                                    }
                                }
                            }
                        })
                    else:
                        main_query.append({
                            key_query: {
                                item.key: value
                            }
                        })
            case "is_not":
                if isinstance(item.value, str):
                    new_value = await decode_value_from_alias(
                        item.value,
                        user_id,
                        role_name
                    )
                    if not new_value.status:
                        return None
                    else:
                        if isinstance(new_value.data, list):
                            key_query = selected_query(item.key, properties, True)
                        else:
                            key_query = selected_query(item.key, properties, False)
                        value = new_value.data
                else:
                    key_query = selected_query(item.key, properties, False)
                    value = item.value

                if key_query:
                    if key_query == "match_phrase" and isinstance(value, list):
                        main_query.append({
                            "bool": {
                                "must_not": {
                                    "bool": {
                                        "should": [{key_query: {item.key: item_value}} for item_value in value]
                                    }
                                }
                            }
                        })
                    else:
                        main_query.append({
                            "bool": {
                                "must_not": {
                                    key_query: {
                                        item.key: value
                                    }
                                }
                            }
                        })
            case "is_one_of":
                list_value = []
                for value in item.value:
                    if isinstance(value, str):
                        new_value = await decode_value_from_alias(
                            value,
                            user_id,
                            role_name
                        )
                        if not new_value.status:
                            return None
                        else:
                            if isinstance(new_value.data, list):
                                list_value += new_value.data
                            else:
                                list_value.append(new_value.data)
                    else:
                        list_value += value

                key_query = selected_query(item.key, properties, True)
                if key_query:
                    if key_query == "match_phrase":
                        main_query.append({
                            "bool": {
                                "must": {
                                    "bool": {
                                        "should": [{key_query: {item.key: item_value}} for item_value in list_value]
                                    }
                                }
                            }
                        })
                    else:
                        main_query.append({
                            key_query: {
                                item.key: list_value
                            }
                        })
            case "is_not_one_of":
                list_value = []
                for value in item.value:
                    if isinstance(value, str):
                        new_value = await decode_value_from_alias(
                            value,
                            user_id,
                            role_name
                        )
                        if not new_value.status:
                            return None
                        else:
                            if isinstance(new_value.data, list):
                                list_value += new_value.data
                            else:
                                list_value.append(new_value.data)
                    else:
                        list_value += value

                key_query = selected_query(item.key, properties, True)
                if key_query:
                    if key_query == "match_phrase":
                        main_query.append({
                            "bool": {
                                "must_not": {
                                    "bool": {
                                        "should": [{key_query: {item.key: item_value}} for item_value in list_value]
                                    }
                                }
                            }
                        })
                    else:
                        main_query.append({
                            "bool": {
                                "must_not": {
                                    key_query: {
                                        item.key: list_value
                                    }
                                }
                            }
                        })
            case "exist":
                main_query.append({
                    "exists": {
                        "field": item.key
                    }
                })
            case "not_exist":
                main_query.append({
                    "bool": {
                        "must_not": {
                            "exists": {
                                "field": item.key
                            }
                        }
                    }
                })
            case "less_than":
                main_query.append({
                    "range": {
                        item.key: {
                            "lt": item.value
                        }
                    }
                })
            case "greater_or_equal":
                main_query.append({
                    "range": {
                        item.key: {
                            "gte": item.value
                        }
                    }
                })
            case "between":
                main_query.append({
                    "range": {
                        item.key: {
                            "gte": item.value.start,
                            "lte": item.value.end
                        }
                    }
                })
            case "not_between":
                main_query.append({
                    "bool": {
                        "must_not": {
                            "range": {
                                item.key: {
                                    "gte": item.value.start,
                                    "lte": item.value.end
                                }
                            }
                        }
                    }
                })

    for item in payload.nested:
        temp = await generate_filter(item, properties, user_id, role_name)
        if temp:
            main_query.append(temp)

    return query
