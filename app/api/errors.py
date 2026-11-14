import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.exceptions import AppError

logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        if exc.status_code >= 500:
            logger.warning(
                "%s %s -> %d: %s", request.method, request.url.path, exc.status_code, exc
            )
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())
