from fastapi import UploadFile

from app.core.exceptions import UnsupportedFileTypeError, UploadTooLargeError


async def read_csv_upload(file: UploadFile, *, max_bytes: int) -> bytes:
    # Content-Type is unreliable (browsers and curl often send application/octet-stream),
    # so the extension is the gate; the parser rejects anything that isn't really CSV.
    if file.filename and not file.filename.lower().endswith(".csv"):
        raise UnsupportedFileTypeError("Only .csv files are accepted")
    content = await file.read(max_bytes + 1)  # never buffer more than the limit
    if len(content) > max_bytes:
        raise UploadTooLargeError(f"File exceeds the {max_bytes}-byte limit")
    return content
