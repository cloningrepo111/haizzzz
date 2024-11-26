import itertools
from collections import defaultdict
import base64
from datetime import datetime, timedelta, timezone
import json
import zlib
import asyncio

from fastapi import Request

from bson import ObjectId
from pymongo.errors import PyMongoError

from src.db import client, SearchStage, SearchPipeline, SearchResult
from src.models.response_schema import ResponseModel
from src.models.search_schema import *

from src.utils.search import generate_id
# from src.utils.validations import get



async def create_pipeline(payload: CreatePipelineModel) -> ResponseModel:
    try:
        inserted_data = payload.model_dump()
        result = await SearchPipeline.insert_one(inserted_data)

        if not result.acknowledged:
            return ResponseModel(status=False,
                                data=None,
                                message="Failure! Please attempt the action again.")
    
        return ResponseModel(status=True,
                                data=None,
                                message="Your request has been fulfilled.")
        
    except Exception as e:
        print(f"Create new pipeline failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def update_pipeline(payload: UpdatePipelineModel) -> ResponseModel:
    try:
        await SearchPipeline.update_many(
            {
                # "user_id": user_id,
                "name": payload.name,
                "_id": {"$ne": ObjectId(payload.id)}
            },
            {"$set": {"name": None}}
        )

        result = await SearchPipeline.update_one(
            {"_id": ObjectId(payload.id)},
            #  "user_id": user_id},
            [{"$set": {"name": payload.name}}],
        )
        if result.modified_count <= 0:
            return ResponseModel(status=False,
                                 data=None,
                                 message="Failure! Only the pipeline name can be modified.")

        return ResponseModel(status=True,
                             data=None,
                             message="Your request has been fulfilled.")
    except Exception as e:
        print(f"Update pipeline failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Something went wrong! Please attempt the action again.")


async def create_stage(payload: CreateStageModel):
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                try:
                    pipeline_id = payload.pipeline_id

                    pipeline = await SearchPipeline.find_one(
                        {"_id": ObjectId(pipeline_id)},
                        session=session
                    )

                    if not pipeline:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                            data=None,
                                            message=f"Failure! No pipeline found for the provided pipeline_id: {pipeline_id}.")

                    # if pipeline["user_id"] != user_id:
                    #     await session.abort_transaction()
                    #     return ResponseModel(status=False,
                    #                         data=None,
                    #                         message="Failure! The pipeline_id provided not belong to current user.")

                    # Parse to original query
                    if len(payload.query.queries) > 0:
                        output_queries = []
                        for q in payload.query.queries:
                            output_query = {}
                            for key, value in q.items():
                                if isinstance(value, str) and "." in value:
                                    splits = value.split('.')
                                    p_id = pipeline_id
                                    if len(splits) == 2:   # s1.src_ip
                                        stage, field = splits
                                    elif len(splits) == 3: # p1.s1.src_ip
                                        pipeline_name, stage, field = splits
                                        p = await SearchPipeline.aggregate([
                                            {"$match": {"name": pipeline_name}},# "user_id": user_id}},
                                            {"$set": {"id": {"$toString": "$_id"}}},
                                            {"$unset": "_id"}
                                        ]).to_list(length=1)
                                        if not p:
                                            return ResponseModel(status=False,
                                                                data=None,
                                                                message=f"Failure! The pipeline name `{pipeline_name}` is missing or undefined")
                                        p_id = p[0]['id']
                                    elif len(splits) == 4:   # IP
                                        output_query[key] = [value]
                                        continue
                                    else:
                                        return ResponseModel(status=False,
                                                            data=None,
                                                            message=f"Failure! Invalid input query detected, please try again.")
                                                            
                                    if key != field:
                                        return ResponseModel(status=False,
                                                            data=None,
                                                            message=f"Failure! Field name does not match. {key} / {field}")
                                    
                                    search_result = await SearchResult.aggregate([
                                        {"$match": {"name": stage, "pipeline_id": p_id}},
                                    ]).to_list(None)

                                    if not search_result:
                                        return ResponseModel(status=False,
                                                             data=None,
                                                             message=f"Failure! Stage name `{stage}` data in search query not found.")

                                    output_query[key] = []
                                    search_result = search_result[0]['data']
                                    for entry in search_result:
                                        d = entry['_source']
                                        if field in d:
                                            output_query[key].append(d[field])
                                    
                                    output_query[key] = list(set(output_query[key]))
                                else:
                                    output_query[key] = [value]

                            print('\nquery')
                            print('q', q)
                            print('before output_query', output_query)
                            output_query = {key: value for key, value in output_query.items() if value}
                            combinations = []
                            if output_query:
                                keys, values = zip(*output_query.items())
                                combinations = [dict(zip(keys, combination)) for combination in itertools.product(*values)]
                                output_queries += combinations
                            print('after output_query', output_query)
                            print('combinations', combinations)

                        payload.query.queries = output_queries
                        print('\noutput_queries', output_queries)

                    # payload.user_id = user_id
                    result = await SearchStage.insert_one(payload.model_dump(), session=session)
                    inserted_id = str(result.inserted_id)

                    pipeline['stage_ids'].append(inserted_id)
                    result = await SearchPipeline.update_one(
                        {"_id": ObjectId(pipeline_id)},
                        {"$set": {"stage_ids": pipeline["stage_ids"]}},
                        session=session
                    )

                    if result.modified_count <= 0:
                        return ResponseModel(status=False,
                                            data=None,
                                            message="Failure! Failed to update the pipeline with the new stage.")
                    
                    return ResponseModel(status=True,
                                        data={'stage_id': inserted_id},
                                        message='Your request has been fulfilled')
                
                except PyMongoError as e:
                    await session.abort_transaction()
                    print(f"Transaction aborted due to error: {e}")
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! An error occurred during the transaction.")
    except Exception as e:
        print(f"Create stage failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def update_stage(payload: UpdateStageModel) -> ResponseModel:
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                try:
                    # Checking
                    r1 = await SearchStage.aggregate([
                        {"$match": {"_id": ObjectId(payload.id)}},
                        {"$project": {"pipeline_id": 1, "result_id": 1}}
                    ], session=session).to_list(length=1)

                    pipeline_id = r1[0].get('pipeline_id')
                    name = payload.name

                    r1 = await SearchStage.aggregate([
                        {
                            "$match": {
                                "name": name,
                                "pipeline_id": pipeline_id
                            }
                        },
                        {"$project": {"name": 1}}
                    ], session=session).to_list(length=1)

                    if r1:
                        r2 = await SearchStage.update_many(
                            {
                                "pipeline_id": pipeline_id,
                                "name": payload.name
                            },
                            [{"$set": {"name": None, "saved": False}}],
                            session=session
                        )

                        r3 = await SearchResult.update_one(
                            {
                                "pipeline_id": pipeline_id,
                                "name": payload.name
                            },
                            [{"$set": {"name": None}, "saved": False}],
                            session=session
                        )

                        if r2.modified_count <= 0 or r3.modified_count <= 0:
                            await session.abort_transaction()
                            return ResponseModel(status=False,
                                                 data=None,
                                                 message="Failure! Please attempt this action again.123213")

                    r4 = await SearchStage.update_one(
                        {
                            "_id": ObjectId(payload.id)
                        },
                        [{"$set": {"name": payload.name}}],
                        session=session
                    )

                    r5 = await SearchResult.update_one(
                        {
                            "stage_id": payload.id
                        },
                        {"$set": {"name": payload.name}},
                        session=session
                    )

                    if r4.modified_count <= 0 or r5.modified_count <= 0:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Failure! Please attempt this action again.")

                    await session.commit_transaction()
                    return ResponseModel(status=True,
                                        data=None,
                                        message="Your request has been fulfilled.")

                except PyMongoError as e:
                    print(f"Transaction aborted due to error: {e}")
                    await session.abort_transaction()
                    return ResponseModel(
                        status=False,
                        data=None,
                        message="Failure! An error occurred during the operation."
                    )

    except Exception as e:
        print(f"Update stage failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def search_query(stage: dict) -> ResponseModel:
    try:
        pipeline_id = stage['pipeline_id']
        
        pipeline = await SearchPipeline.aggregate([
            {"$match": {"_id": ObjectId(pipeline_id)}},
            {"$limit": 1}
        ]).to_list(length=1)

        if not pipeline:
            return ResponseModel(status=False,
                                 data=None,
                                 message=f"Failure! Not found pipeline_id: {pipeline_id}.")
    
        pipeline = pipeline[0]
        indexs = ['CNTT', 'MKT']
        # if len(pipeline['source']) > 0:
        #     indexs = await get_indexs_by_resource(pipeline['source'])
        # else:
        #     indexs = pipeline['tenant']
        
        # if not indexs:
        #     return ResponseModel(status=False,
        #                          data=None,
        #                          message="Failure! Check your inputs and try again.")
        
        # Get time
        if pipeline['time']['get_relative']:
            end = datetime.now(timezone.utc)
        if pipeline['time']['relative']['unit'] == "m":
            start = end - timedelta(minutes=pipeline['time']['relative']['time'])
        elif pipeline['time']['relative']['unit'] == "h":
            start = end - timedelta(hours=pipeline['time']['relative']['time'])
        else:
            start = end - timedelta(days=pipeline['time']['relative']['time'])
                
        query = {"bool": {"must": [], "must_not": [], "filter": []}}
        query["bool"]["must"].append({
            "range": {
                "@timestamp": {
                    "gte": start.isoformat(),
                    "lte": end.isoformat()
                }
            }
        })

        # mapping = await Mapping.aggregate(pipeline=DEFAULT_MAPPING_PIPELINE).to_list(length=1)
        # if len(mapping) > 0:
        #     mapping = mapping[0]
        # else:
        #     return ResponseModel(status=False,
        #                         data=None,
        #                         message="Failure! Missing mapping configuration, please contact with admin")
        # properties = mapping.get("properties")

        # if pipeline['stage_ids'][-1] != str(stage['_id']):
        #     return ResponseModel(status=False,
        #                          data=None,
        #                          message="Failure! There are no stages to run.")

        # # Parse stage.query.queries
        # if len(stage['query']['queries']) > 0:
        #     queries = await parse_queries(stage['query']['queries'], properties, user_id, role_name)
        #     if not queries.status:
        #         return queries
        #     query["bool"]["must"].append({"bool": {"should": queries.data}})

        # # Parse search.query.filters
        # for field in stage['query']['filters']:
        #     gen_fils = await generate_filter(field, properties, user_id, role_name)
        #     if gen_fils:
        #         if field.include:
        #             query["bool"]["filter"].append(gen_fils)
        #         else:
        #             query["bool"]["must_not"].append(gen_fils)
        #     else:
        #         return ResponseModel(status=False,
        #                              data=None,
        #                              message="Failure! Check your inputs and try again.")

        # # Parse ids
        # if len(stage['query']['ids']) > 0:
        #     query["bool"]["must"].append({"ids": {"values": stage['query']['ids']}})
        
        # # Search data
        # if not stage['after']:
        #     data = await es.search(
        #         index=indexs,
        #         query=query,
        #         size=stage['limit'],
        #         sort=[{stage['sorted']: stage['order']}],
        #         source_includes=stage['query']['includes'] if len(stage['query']['includes']) > 0 else None,
        #         source_excludes=stage['query']['excludes'] if len(stage['query']['excludes']) > 0 else None,
        #         filter_path=["hits.hits._id", "hits.hits._source", "hits.hits.sort"],
        #         search_after=stage['after']
        #     )
        # else:
        #     data = await es.search(
        #         index=indexs,
        #         query=query,
        #         size=stage['limit'],
        #         sort=[{stage['sorted']: stage['order']}],
        #         source_includes=stage['query']['includes'] if len(stage['query']['includes']) > 0 else None,
        #         source_excludes=stage['query']['excludes'] if len(stage['query']['excludes']) > 0 else None,
        #         filter_path=['hits.hits._id', 'hits.hits._source', 'hits.hits.sort'],
        #     )

        # if data.body.get('hits'):
        #     data = data.body['hits']['hits']
        # else:
        #     data = data.body
        
        # if stage['compress']:
        #     data = zlib.compress(json.dumps({"data": data}).encode())
        #     data = base64.b64encode(data).decode()

        data = [
            {"_source": {'src_ip': '123', 'dest_ip': '456'}},
            {"_source": {'src_ip': '123a', 'dest_ip': '456a'}},
            {"_source": {'src_ip': '123b', 'dest_ip': '456b'}}
        ]

        data = [
            {"_source": {'src_ip': 'ababac', 'dest_ip': 'ababac', 'protocol': "TCP"}},
            {"_source": {'src_ip': '123a', 'dest_ip': 'ababac', 'protocol': "ICMP"}},
            {"_source": {'src_ip': 'ababac', 'dest_ip': 'ababac', "ack_count": 20}}
        ]

        cached_result = {
            'pipeline_id': pipeline_id,
            'stage_id': str(stage['_id']),
            'name': stage.get('name', None),
            'saved': stage.get('saved', None),
            'data': data
        }
        result = await SearchResult.insert_one(cached_result)

        await SearchStage.update_one(
            {"_id": stage["_id"]},
            {"$set": {"result_id": str(result.inserted_id)}}
        )

        if not result.acknowledged:
            return ResponseModel(status=False,
                                    data=None,
                                    message='Failure! Cannot cache the search result, please attempt the action again.')

        return ResponseModel(status=False,
                             data=cached_result,
                             message='Your request has been fulfilled.')

    except Exception as e:
        print(f'Search failed: {e}')
        return ResponseModel(status=False,
                             data=None,
                             message='Failure! Please attempt the action again.')
    

async def clean_up(pipeline_id: str) -> ResponseModel:
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                try:
                    pipeline = await SearchPipeline.aggregate([
                        {"$match": {"_id": ObjectId(pipeline_id)}},
                        {"$limit": 1}
                    ]).to_list(length=1)

                    if not pipeline:
                        return ResponseModel(
                            status=False,
                            data=None,
                            message="Failure! Not found pipeline_id."
                        )
                    pipeline = pipeline[0]
                    
                    if not pipeline['saved']:
                        r1 = await SearchResult.delete_many(
                            {"pipeline_id": pipeline_id},
                            session=session
                        )
                        r2 = await SearchStage.delete_many(
                            {"pipeline_id": pipeline_id},
                            session=session
                        )
                        r3 = await SearchPipeline.delete_one(
                            {"_id": ObjectId(pipeline_id)},
                            session=session
                        )

                        if r1.deleted_count <= 0 or r2.deleted_count <= 0 or r3.deleted_count <= 0:
                            await session.abort_transaction()
                            return ResponseModel(
                                status=False,
                                data=None,
                                message="Failure! Some records could not be deleted. 1"
                            )
                    else:
                        r1 = await SearchResult.delete_many(
                            {"pipeline_id": pipeline_id, "saved": False},
                            session=session
                        )

                        deleted_stage_ids = await SearchStage.aggregate([
                            {"$match": {"pipeline_id": pipeline_id, "saved": False}},
                            {"$project": {"_id": 1}}
                        ]).to_list(length=None)
                        deleted_stage_ids = [doc["_id"] for doc in deleted_stage_ids]

                        r2 = await SearchStage.delete_many(
                            {"_id": {"$in": deleted_stage_ids}},
                            session=session
                        )
                        
                        pipeline['stage_ids'] = [ObjectId(stage_id) for stage_id in pipeline['stage_ids']]
                        pipeline['stage_ids'] = [str(stage_id) for stage_id in pipeline['stage_ids'] if stage_id not in deleted_stage_ids]

                        r3 = await SearchPipeline.update_one(
                            {"_id": ObjectId(pipeline_id)},
                            {"$set": {"stage_ids": pipeline["stage_ids"]}},
                            session=session
                        )

                        if r1.deleted_count <= 0 or r2.deleted_count <= 0 or r3.modified_count <= 0:
                            await session.abort_transaction()
                            return ResponseModel(
                                status=False,
                                data=None,
                                message="Failure! Some records could not be deleted. 2"
                            )

                    await session.commit_transaction()
                    return ResponseModel(status=True,
                                         data=None,
                                         message="Your request has been fulliled.")

                except PyMongoError as e:
                    await session.abort_transaction()
                    print(f"Transaction aborted due to error: {e}")
                    return ResponseModel(status=False,
                                         data=None,
                                         message="Failure! An error occurred during the transaction.")
    except Exception as e:
        print(f"Clean up cache failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def search_result_generator(request: Request, pipeline_id: str):
    try:
        cs = SearchStage.watch(pipeline=[
            {
                "$match": {
                    "operationType": "insert",
                    "fullDocument.pipeline_id": pipeline_id
                }
            }
        ])

        while True:
            if await request.is_disconnected():
                print('SSE disconnected')
                cleanup_task = asyncio.create_task(clean_up(pipeline_id))
                asyncio.shield(cleanup_task)
                print('Clean up has been done')
                await cs.close()
                return
            
            stage = await cs.try_next()
            if stage:
                result = await search_query(stage['fullDocument'])
                yield result
            
    except asyncio.CancelledError:
        print(f"Client disconnected while streaming pipeline: {pipeline_id}")
        cleanup_task = asyncio.create_task(clean_up(pipeline_id))
        asyncio.shield(cleanup_task)
        print('Clean up has been done')
        raise
    
    # except Exception as e:
    #     print(f"Error: {e}")
    #     result = result.model_dump()
    #     yield f"data: {json.dumps(ResponseModel(status=False, data=None, message='Failure! Please attempt the action again.'))}\n\n"

    finally:
        print("CS close")
        await cs.close()
