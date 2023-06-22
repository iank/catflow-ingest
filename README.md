# catflow-ingest

Data ingestion endpoint for an object recognition pipeline

Accepts video, uploads to S3, and sends a task to a queue.

# Setup

* Install [pre-commit](https://pre-commit.com/#install) in your virtualenv. Run
`pre-commit install` after cloning this repository.

# Develop

```
pip install --editable .[dev]

export S3_ENDPOINT_URL="your-endpoint-url"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_BUCKETNAME="your-bucket-name"
export RABBITMQ_URL="amqp://username:password@hostname:port/"
export RABBITMQ_EXCHANGE="catflow-ingest"
uvicorn catflow_ingest.main:app --reload
```

## Test

Note: some tests start a rabbitmq-server and require that to be installed.

```
pytest
```

# Build

```
pip install build
python -m build
docker build -t iank1/catflow_ingest:latest .
```

Note that the docker build step will currently fail if there is more than one wheel version in `dist/`

# Deploy

See [catflow-docker](https://github.com/iank/catflow-docker) for `docker-compose.yml`

# Example

```
curl -X POST "https://localhost/ingest/" -H "Content-Type: multipart/form-data" -F "file=@file.mp4"|jq
```
