FROM python:3.7.12-slim

RUN apt-get update && apt-get install -y \
    python3-pip \
    git \
    gnupg \
    wget \
    mongodb-org

RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list && \
    apt-get update && apt-get install -y mongodb-org

RUN pip3 install --upgrade pip && \
    pip3 install fastapi uvicorn pydantic pymongo transformers torch jieba python-dotenv requests bson

COPY ./linkinpark /pylib/linkinpark/
COPY ./setup.py /pylib/setup.py

WORKDIR /pylib/
RUN pip3 install .

ENV PYTHONPATH=$PYTHONPATH:/pylib/

WORKDIR /app

# 假設您的應用入口點是 main.py
COPY ./main.py /app/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
