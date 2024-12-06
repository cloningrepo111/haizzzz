from sse_starlette.sse import EventSourceResponse
from fastapi import APIRouter, Depends, status

# from src.authentication import oauth2

from src.models.response_schema import ResponseModel
from src.models.search_schema import *

from src.controllers.search_controller import *

from src.utils.search import *


router = APIRouter(
    prefix='/search',
    tags=['search'],
    responses={404: {'description': 'Not found'}}
)


@router.post('/pipeline', status_code=status.HTTP_200_OK, response_model=ResponseModel)
async def create_pipeline_endpoint(payload: CreatePipelineModel):#, user_id: str #= Depends(oauth2.require_user)):
    # set_endpoint_privileges = ['all', 'read']
    # check_role_name = await check_user_role(
    #     user_id,
    #     SET_SYSTEM_NAME,
    #     set_endpoint_privileges
    # )
    # if not check_role_name.status:
    #     return check_role_name
    # else:
    result = await create_pipeline(payload)
    return result


@router.put("/pipeline", status_code=status.HTTP_200_OK, response_model=ResponseModel)
async def update_pipeline_endpoint(payload: UpdatePipelineModel):
    result = await update_pipeline(payload)
    return result


@router.post('/pipeline/stage', status_code=status.HTTP_200_OK, response_model=ResponseModel)
async def create_stage_endpoint(payload: CreateStageModel):# = Depends(oauth2.require_user)):
    result = await create_stage(payload)
    return result


@router.put('/pipeline/stage', status_code=status.HTTP_200_OK, response_model=ResponseModel)
async def update_stage_endpoint(payload: UpdateStageModel):# = Depends(oauth2.require_user)):
    result = await update_stage(payload)
    return result


@router.get('/pipeline/sse/results', status_code=status.HTTP_200_OK, response_model=ResponseModel)
async def stream_results_endpoint(request: Request, pipeline_id: str):#: str = Depends(oauth2.require_user)):
    return EventSourceResponse(
        search_result_generator(
            request,
            pipeline_id
        ),
        ping=5
    )


# @router.get("/pipeline/{pipeline_id}/results/{stage_name}", status_code=status.HTTP_200_OK, response_model=ResponseModel)
# async def get_stage_result_endpoint(pipeline_id: str, stage_name: str):
#     result = await get_stage_result(pipeline_id, stage_name)
#     return result

