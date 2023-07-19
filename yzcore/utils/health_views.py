from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get('/health')
async def health():
    """用于服务健康检测"""
    return JSONResponse(content="OK")
