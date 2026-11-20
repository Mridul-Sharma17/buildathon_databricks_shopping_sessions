FROM python:3.12-slim-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends g++
COPY pipeline/normalize_csv.cpp /src/normalize_csv.cpp
RUN g++ -std=c++11 -O2 -static-libgcc -static-libstdc++ /src/normalize_csv.cpp -o /normalize_csv

FROM python:3.12-slim-bookworm

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=build /normalize_csv /usr/local/bin/normalize_csv
COPY pipeline/pipeline.py pipeline/baseline.sql /app/pipeline/

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/scratch TMPDIR=/scratch
USER 1000:1000
CMD ["python3", "/app/pipeline/pipeline.py"]
