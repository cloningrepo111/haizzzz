import asyncio
import datetime
import json
import base64
import zlib

from fastapi import Request

from config import settings
from pymongo.errors import PyMongoError

from bson import ObjectId
from src.db import client, SearchStage, SearchPipeline, SearchResult
from src.models.response_schema import ResponseModel
from src.models.search_schema import \
    StageModel, CreateStageModel, UpdateStageModel, \
    PipelineModel, CreatePipelineModel

from src.utils.search import generate_id
# from src.utils.validations import get


async def create_pipeline(payload: CreatePipelineModel):
    try:
        # Check session
        session_id = payload.session_id
        if not session_id:
            payload.session_id = await generate_id()

            inserted_data = payload.model_dump()
            result = await SearchPipeline.insert_one(inserted_data)

            if not result.acknowledged:
                return ResponseModel(status=False,
                                    data=None,
                                    message="Failure! Please attempt the action again.")
        
            return ResponseModel(status=True,
                                 data=None,
                                 message="Your request has been fulfilled.")
        else:
            pipeline = await SearchPipeline.aggregate([
                {"$match": {"session_id": session_id}},
                {"$set": {"id": {"$toString": "$_id"}}},
                {"$unset": "_id"}
            ]).to_list(length=1)

            if not pipeline:
                return ResponseModel(status=False,
                                     data=None,
                                     message=f"Failure! No pipeline found for the provided session_id: {session_id}.")
            
            return ResponseModel(status=False,
                                 data=pipeline[0],
                                 message=f"Failure! Pipeline already exists for the provided session_id: {session_id}.")
    except Exception as e:
        print(f"Create new pipeline failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def create_stage(payload: CreateStageModel, pipeline_id: str, user_id: str):
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                try:
                    pipeline = await SearchPipeline.find_one(
                        {"_id": ObjectId(pipeline_id)},
                        session=session
                    )
                    if not pipeline:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                            data=None,
                                            message=f"Failure! No pipeline found for the provided pipeline_id: {pipeline_id}.")

                    if pipeline["user_id"] != user_id:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                            data=None,
                                            message="Failure! The pipeline_id provided not belong to current user.")

                    # Parse to original query
                    if len(payload.query.queries) > 0:
                        output_queries = []
                        for q in payload.query.queries:
                            for key, value in q.items():
                                if "." in value:
                                    stage, field = value.split('.')
                                    if key != field:
                                        await session.abort_transaction()
                                        return ResponseModel(status=False,
                                                            data=None,
                                                            message=f"Failure! Field name not macthed. {key} / {field}")
                                    
                                    search_result = await SearchResult.aggregate([
                                        {"$match": {"stage_name": stage, "pipeline_id": pipeline_id}},
                                        {"$limit": 1}
                                    ]).to_list(length=1)

                                    print(search_result)

                                    if not search_result:
                                        await session.abort_transaction()
                                        return ResponseModel(status=False,
                                                            data=None,
                                                            message=f"Failure! Stage name `{stage}` data in query not found.")
                                    else:
                                        search_result = search_result[0]['data']
                                        for entry in search_result:
                                            if field in entry:
                                                output_queries.append({key: entry[field]})
                                else:
                                    output_queries.append({key: value})
                        payload.query.queries = output_queries
                    
                    print(payload.query.queries)
                
                    # # Properties
                    # mapping = await Mapping.aggregate(pipeline=DEFAULT_MAPPING_PIPELINE).to_list(length=1)
                    # if len(mapping) <= 0:
                    #     return ResponseModel(status=False,
                    #                                 data=None,
                    #                                 message="Missing mapping configuration, please contact with admin")
                    # properties = mapping[0].get("properties")
                    
                    # includes_checked = validate_field_for_query(payload.query.includes, properties)
                    # if not includes_checked.status:
                    #     return includes_checked

                    # excludes_checked = validate_field_for_query(payload.query.excludes, properties)
                    # if not excludes_checked.status:
                    #     return excludes_checked

                    payload.pipeline_id = pipeline_id
                    result = await SearchStage.insert_one(payload.model_dump(), session=session)

                    pipeline['stage_ids'].append(str(result.inserted_id))
                    result = await SearchPipeline.update_one(
                        {"_id": ObjectId(pipeline_id)},
                        {"$set": {"stage_ids": pipeline["stage_ids"]}},
                        session=session
                    )

                    if result.modified_count <= 0:
                        return ResponseModel(status=False,
                                            data=None,
                                            message="Failure! Failed to update the pipeline with the new stage.")
                    
                    await session.commit_transaction()

                    return ResponseModel(status=True,
                                        data=None,
                                        message="Stage created successfully.")
                except PyMongoError as e:
                    await session.abort_transaction()  # Abort the transaction on error
                    print(f"Transaction aborted due to error: {e}")
                    return ResponseModel(
                        status=False,
                        data=None,
                        message="Failure! An error occurred during the operation."
                    )
    except Exception as e:
        print(f"Create stage failed: {e}")
        return ResponseModel(status=False,
                             data=None,
                             message="Failure! Please attempt the action again.")


async def update_stage(payload: UpdateStageModel, user_id: str):
    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                try:
                    result = await SearchStage.update_one(
                        {"_id": ObjectId(payload.id)},
                        {"$set": {"stage_name": payload.stage_name}},
                        session=session
                    )

                    if result.modified_count <= 0:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Stage not found.")

                    result = await SearchResult.update_one(
                        {"stage_id": payload.id},
                        {"$set": {"stage_name": payload.stage_name}},
                        session=session
                    )

                    if result.modified_count <= 0:
                        await session.abort_transaction()
                        return ResponseModel(status=False,
                                             data=None,
                                             message="Stage not found.")

                    await session.commit_transaction()
                    return ResponseModel(status=True,
                                        data=None,
                                        message="Successfully.")

                except PyMongoError as e:
                    await session.abort_transaction()
                    print(f"Transaction aborted due to error: {e}")
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


async def search_query(stage: dict, user_id: str, role_name: str) -> ResponseModel:
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
            end = datetime.datetime.now(datetime.timezone.utc)
        if pipeline['time']['relative']['unit'] == "m":
            start = end - datetime.timedelta(minutes=pipeline['time']['relative']['time'])
        elif pipeline['time']['relative']['unit'] == "h":
            start = end - datetime.timedelta(hours=pipeline['time']['relative']['time'])
        else:
            start = end - datetime.timedelta(days=pipeline['time']['relative']['time'])
                
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

        if pipeline['stage_ids'][-1] != str(stage['_id']):
            return ResponseModel(status=False,
                                 data=None,
                                 message="Failure! There are no stages to run.")

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
            {'src_ip': '123', 'dest_ip': '456'},
            {'src_ip': '123a', 'dest_ip': '456a'},
            {'src_ip': '123b', 'dest_ip': '456b'}
        ]

        cached_result = {
            'pipeline_id': pipeline_id,
            'stage_id': str(stage['_id']),
            'stage_name': stage.get('stage_name', None),
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
                             data=data,
                             message='Your request has been fulfilled.')

    except Exception as e:
        print(f'Search failed: {e}')
        return ResponseModel(status=False,
                             data=None,
                             message='Failure! Please attempt the action again.')


async def search_result_generator(request: Request, pipeline_id: str, user_id: str, role_name: str):
    
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
                print('SSE request disconnected')
                await cs.close()
                return
            
            stage = await cs.try_next()
            if stage:
                result = await search_query(stage['fullDocument'], user_id, role_name)
                result = result.model_dump()
                yield json.dumps(result)
            
    except asyncio.CancelledError:
        print(f"Client disconnected while streaming pipeline {pipeline_id}")
        raise
    
    except Exception as e:
        print(f"Error: {e}")
        result = ResponseModel(status=False,
                               data=None,
                               message='Failure! Please attempt the action again.')
        result = result.model_dump()
        yield f"data: {json.dumps(result)}\n\n"
    
    finally:
        print('Clean up things')
        await cs.close()
