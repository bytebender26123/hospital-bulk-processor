from typing import Annotated

from fastapi import Depends
from starlette.requests import HTTPConnection

from app.config import Settings
from app.services.bulk_service import BulkHospitalService
from app.services.progress import ProgressBroker

# HTTPConnection (not Request) so the same dependencies work for WebSocket routes.


def get_settings(conn: HTTPConnection) -> Settings:
    settings: Settings = conn.app.state.settings
    return settings


def get_bulk_service(conn: HTTPConnection) -> BulkHospitalService:
    service: BulkHospitalService = conn.app.state.bulk_service
    return service


def get_progress_broker(conn: HTTPConnection) -> ProgressBroker:
    broker: ProgressBroker = conn.app.state.progress_broker
    return broker


SettingsDep = Annotated[Settings, Depends(get_settings)]
BulkServiceDep = Annotated[BulkHospitalService, Depends(get_bulk_service)]
ProgressBrokerDep = Annotated[ProgressBroker, Depends(get_progress_broker)]
