FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ./ingestion/
COPY dbt/ ./dbt/
COPY dashboards/ ./dashboards/
COPY ml/ ./ml/

RUN mkdir -p cache secrets

EXPOSE 8501

CMD ["streamlit", "run", "dashboards/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0"]