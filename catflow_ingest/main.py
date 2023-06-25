from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from . import _version
from .producer import Producer
import aioboto3
import os
import json
from uuid import uuid4
import logging


async def startup_event():
    app.state.producer = await Producer.create(
        os.environ["RABBITMQ_URL"],
        os.environ["RABBITMQ_EXCHANGE"],
    )


async def shutdown_event():
    await app.state.producer.close()


logger = logging.getLogger(__name__)
app = FastAPI()
app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)

# Routing keys
INGEST_KEY = "ingest.video"
DETECT_KEY = "detect.video"


async def check_rabbitmq_connection() -> bool:
    if app.state.producer.connection.is_closed:
        return False

    return True


@app.get("/status")
async def status():
    rmq_status = await check_rabbitmq_connection()
    status = {"version": _version.version, "rabbitmq_status": rmq_status}

    if not rmq_status:
        return JSONResponse(status, status_code=500)

    return status


async def upload_to_s3(file: bytes, filename: str):
    session = aioboto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    async with session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"]) as s3:
        await s3.upload_fileobj(file, os.environ["AWS_BUCKETNAME"], filename)


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    s3_path = str(uuid4()) + "." + file.filename.split(".")[-1]
    try:
        await upload_to_s3(file, s3_path)
        await app.state.producer.send_to_rabbitmq(INGEST_KEY, json.dumps([s3_path]))
        await app.state.producer.send_to_rabbitmq(DETECT_KEY, json.dumps([s3_path]))
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
