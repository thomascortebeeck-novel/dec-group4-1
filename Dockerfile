FROM python:3.9-slim-bookworm

# Install gcc and python3-dev
RUN apt-get update && apt-get install -y gcc python3-dev

WORKDIR /app
COPY /app .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipeline.pipelines"]
