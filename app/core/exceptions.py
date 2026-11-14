from typing import Any


class AppError(Exception):
    """Base class for errors that map cleanly onto an HTTP response."""

    status_code: int = 500

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail

    def to_dict(self) -> dict[str, Any]:
        return {"detail": self.detail}


class JobNotFoundError(AppError):
    status_code = 404


class JobConflictError(AppError):
    status_code = 409


class UploadTooLargeError(AppError):
    status_code = 413


class UnsupportedFileTypeError(AppError):
    status_code = 415
