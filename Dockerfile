FROM python:3.10 as base

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir .

EXPOSE 5057

CMD ["uvicorn", "catflow_ingest.main:app", "--port", "5057", "--host", "0.0.0.0"]
