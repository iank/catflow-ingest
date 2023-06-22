from fastapi import FastAPI, File, UploadFile, HTTPException
from . import _version
import aioboto3
from aio_pika import connect_robust, Message
import os
from uuid import uuid4
import logging

logger = logging.getLogger(__name__)
app = FastAPI()


async def check_rabbitmq_connection() -> bool:
    try:
        connection = await connect_robust(os.environ["RABBITMQ_URL"])
        await connection.close()
        return True
    except Exception:
        return False


@app.get("/status")
async def status():
    rmq_status = await check_rabbitmq_connection()
    return {"version": _version.version, "rabbitmq_status": rmq_status}


async def upload_to_s3(file: bytes, filename: str):
    session = aioboto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    async with session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"]) as s3:
        await s3.upload_fileobj(file, os.environ["AWS_BUCKETNAME"], filename)


# TODO: connect/declare once w/ Producer class
async def send_to_rabbitmq(exchange_name: str, message: str):
    connection = await connect_robust(os.environ["RABBITMQ_URL"])
    channel = await connection.channel()
    exchange = await channel.declare_exchange(exchange_name, "fanout")
    await exchange.publish(Message(body=message.encode("utf-8")), "")
    await connection.close()


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    s3_path = str(uuid4()) + "." + file.filename.split(".")[-1]
    try:
        await upload_to_s3(file, s3_path)
        await send_to_rabbitmq(os.environ["RABBITMQ_EXCHANGE"], s3_path)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
