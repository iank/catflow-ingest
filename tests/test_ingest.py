import pytest
from httpx import AsyncClient
import catflow_ingest
import aioboto3
from uuid import UUID
from io import BytesIO
import os
import aiofile

from .mock_server import start_service
from .mock_server import stop_process

os.environ["S3_ENDPOINT_URL"] = "http://localhost:5002"
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost"
os.environ["RABBITMQ_EXCHANGE"] = "pytest-exchange"
os.environ["AWS_ACCESS_KEY_ID"] = "pytest-access-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "pytest-secret-key"
os.environ["AWS_BUCKETNAME"] = "pytest-bucket"


@pytest.fixture(scope="session")
def s3_server():
    host = "localhost"
    port = 5002
    url = "http://{host}:{port}".format(host=host, port=port)
    process = start_service("s3", host, port)
    yield url
    stop_process(process)


@pytest.mark.asyncio
async def test_upload_to_s3(s3_server):
    # Set up bucket
    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=s3_server) as s3:
        await s3.create_bucket(Bucket=os.environ["AWS_BUCKETNAME"])

        # Test
        file = BytesIO(b"Hello, world!")
        await catflow_ingest.main.upload_to_s3(file, "testfile.txt")

        # Check result
        response = await s3.get_object(
            Bucket=os.environ["AWS_BUCKETNAME"], Key="testfile.txt"
        )
        response_data = await response["Body"].read()
        assert response_data == b"Hello, world!"


@pytest.mark.asyncio
async def test_send_to_rabbitmq(rabbitmq):
    """Test that send_to_rabbitmq sends a message to an exchange"""
    # Mock setup
    rmq_port = rabbitmq._impl.params.port
    rabbitmq_url = f"amqp://guest:guest@localhost:{rmq_port}/"
    channel = rabbitmq.channel()
    channel.exchange_declare(exchange="test-exchange", exchange_type="direct")
    channel.queue_declare("testkey_queue")
    channel.queue_bind(
        exchange="test-exchange", queue="testkey_queue", routing_key="testkey"
    )

    # Object under test
    producer = await catflow_ingest.Producer.create(
        rabbitmq_url, "test-exchange", ["testkey"]
    )
    await producer.send_to_rabbitmq("testkey", "test message")
    await producer.close()

    # Verify
    _, _, body = channel.basic_get("testkey_queue")
    assert body.decode() == "test message"


@pytest.mark.asyncio
async def test_ingest_endpoint(rabbitmq, s3_server):
    """Test that a video uploaded to /ingest is sent to the infer and ingest queues"""
    # Set up mock rabbitmq
    rmq_port = rabbitmq._impl.params.port
    os.environ["RABBITMQ_URL"] = f"amqp://guest:guest@localhost:{rmq_port}/"
    channel = rabbitmq.channel()
    channel.exchange_declare(
        exchange=os.environ["RABBITMQ_EXCHANGE"], exchange_type="direct"
    )
    channel.queue_declare("infer_queue")
    channel.queue_bind(
        exchange=os.environ["RABBITMQ_EXCHANGE"],
        queue="infer_queue",
        routing_key="infer",
    )
    channel.queue_declare("ingest_queue")
    channel.queue_bind(
        exchange=os.environ["RABBITMQ_EXCHANGE"],
        queue="ingest_queue",
        routing_key="ingest",
    )

    # Set up mock S3
    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=s3_server) as s3:
        await s3.create_bucket(Bucket=os.environ["AWS_BUCKETNAME"])

        # Post car.mp4 to /ingest
        with open("tests/test_images/car.mp4", "rb") as video_file:
            file_data = {"file": video_file}
            async with AsyncClient(
                app=catflow_ingest.main.app, base_url="http://test"
            ) as client:
                await catflow_ingest.main.app.router.startup()
                response = await client.post("/ingest", files=file_data)
                await catflow_ingest.main.app.router.shutdown()

            assert response.status_code == 200, response.json()
            data = response.json()
            assert data["status"] == "success"

        # Check that the message was sent
        _, _, infer_body = channel.basic_get("infer_queue")
        _, _, ingest_body = channel.basic_get("ingest_queue")
        assert infer_body == ingest_body, "Same message was sent to both queues"

        s3_filename = infer_body.decode()
        uuid, ext = s3_filename.split(".")
        assert ext == "mp4"
        try:
            UUID(uuid)
        except ValueError:
            pytest.fail("The S3 filename was not a valid UUID")

        # Check that the file was uploaded to S3
        response = await s3.get_object(
            Bucket=os.environ["AWS_BUCKETNAME"], Key=s3_filename
        )
        s3_content = await response["Body"].read()
        async with aiofile.AIOFile("tests/test_images/car.mp4", "rb") as afp:
            local_content = await afp.read()
            # Check if the contents are equal
            assert s3_content == local_content


@pytest.mark.asyncio
async def test_status_endpoint(rabbitmq):
    rmq_port = rabbitmq._impl.params.port
    os.environ["RABBITMQ_URL"] = f"amqp://guest:guest@localhost:{rmq_port}/"

    async with AsyncClient(
        app=catflow_ingest.main.app, base_url="http://test"
    ) as client:
        await catflow_ingest.main.app.router.startup()
        response = await client.get("/status")
        await catflow_ingest.main.app.router.shutdown()

    assert response.status_code == 200, response.json()
    data = response.json()
    assert data["rabbitmq_status"] is True
    assert len(data["version"].split(".")) >= 3


@pytest.mark.asyncio
async def test_status_endpoint_fails_correctly():
    os.environ["RABBITMQ_URL"] = "amqp://please_fail:guest@localhost:48302/"

    async with AsyncClient(
        app=catflow_ingest.main.app, base_url="http://test"
    ) as client:
        try:
            await catflow_ingest.main.app.router.startup()
        except Exception:
            pass  # We expect this to fail..
        response = await client.get("/status")
        await catflow_ingest.main.app.router.shutdown()

    assert response.status_code == 500
    data = response.json()
    assert data["rabbitmq_status"] is False, data
    assert len(data["version"].split(".")) >= 3
