from fastapi import APIRouter
from src.endpoints.search_endpoint import router as search_router

router = APIRouter(prefix='/api')
router.include_router(search_router)