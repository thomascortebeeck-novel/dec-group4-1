FROM python:3.9-slim-bookworm

# Install gcc and python3-dev
RUN apt-get update && apt-get install -y gcc python3-dev

WORKDIR /app
COPY etl_project /app/etl_project
COPY etl_project_sql_10 /app/etl_project_sql_10
COPY wrapper_script.sh .
COPY requirements.txt .

RUN pip install -r requirements.txt
RUN chmod +x wrapper_script.sh

CMD ["sh", "-c", "./wrapper_script.sh"]