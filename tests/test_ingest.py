import pytest
from fastapi.testclient import TestClient
import catflow_ingest
import aioboto3
from uuid import UUID
from io import BytesIO
import os
import json
import aiofile


from .mock_server import start_service
from .mock_server import stop_process

# Create a test client instance to use in the tests
client = TestClient(catflow_ingest.main.app)

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
async def test_check_rabbitmq_connection_fails():
    """Test that check_rabbitmq_connection fails if it can't connect"""
    assert await catflow_ingest.main.check_rabbitmq_connection() is False


@pytest.mark.asyncio
async def test_check_rabbitmq_connection(rabbitmq):
    """Test that check_rabbitmq_connection succeeds if rabbitmq started"""
    rmq_port = rabbitmq._impl.params.port
    os.environ["RABBITMQ_URL"] = f"amqp://guest:guest@localhost:{rmq_port}/"
    assert await catflow_ingest.main.check_rabbitmq_connection() is True


@pytest.mark.asyncio
async def test_send_to_rabbitmq(rabbitmq):
    """Test that send_to_rabbitmq sends a message to an exchange"""
    rmq_port = rabbitmq._impl.params.port
    os.environ["RABBITMQ_URL"] = f"amqp://guest:guest@localhost:{rmq_port}/"

    # Create exchange and queue, bind
    channel = rabbitmq.channel()
    channel.exchange_declare(exchange="test-exchange", exchange_type="fanout")
    result = channel.queue_declare("", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="test-exchange", queue=queue_name)

    await catflow_ingest.main.send_to_rabbitmq("test-exchange", "test message")
    _, _, body = channel.basic_get(queue_name)
    assert body.decode() == "test message"


@pytest.mark.asyncio
async def test_ingest_endpoint(rabbitmq, s3_server):
    # Set up rabbitmq
    rmq_port = rabbitmq._impl.params.port
    os.environ["RABBITMQ_URL"] = f"amqp://guest:guest@localhost:{rmq_port}/"
    channel = rabbitmq.channel()
    channel.exchange_declare(
        exchange=os.environ["RABBITMQ_EXCHANGE"], exchange_type="fanout"
    )
    result = channel.queue_declare("", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=os.environ["RABBITMQ_EXCHANGE"], queue=queue_name)

    # Set up mock S3
    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=s3_server) as s3:
        await s3.create_bucket(Bucket=os.environ["AWS_BUCKETNAME"])

        # Post car.mp4 to /ingest
        with open("tests/test_images/car.mp4", "rb") as video_file:
            file_data = {"file": video_file}
            response = client.post("/ingest", files=file_data)
            assert response.status_code == 200
            data = json.loads(response.content)
            assert data["status"] == "success"

        # Check that the message was sent
        _, _, body = channel.basic_get(queue_name)
        s3_filename = body.decode()
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

    response = client.get("/status")
    assert response.status_code == 200
    data = json.loads(response.content)
    assert data["rabbitmq_status"] is True
    assert len(data["version"].split(".")) >= 3
