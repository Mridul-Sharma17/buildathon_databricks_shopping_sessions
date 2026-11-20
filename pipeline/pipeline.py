"""Run the Shopping Sessions pipeline and write the two Parquet tables."""

from concurrent.futures import ThreadPoolExecutor
import csv
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import duckdb


INPUT_COLUMNS = {
    "events": "event_id user_id ts ingest_ts event_type product_id price_cents",
    "clicks": "click_id user_id campaign_id ts",
    "links": "user_id_a user_id_b",
    "products": "product_id category",
    "campaigns": "campaign_id channel",
}


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def csv_scan(paths, columns, newline=r"\n"):
    files = ", ".join(sql_string(path) for path in paths)
    schema = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
    return (
        f"SELECT * FROM read_csv([{files}], columns={{{schema}}}, "
        f"auto_detect=false, header=true, new_line={sql_string(newline)}, "
        "hive_partitioning=false)"
    )


def prepare_inputs(con, data, work, threads):
    normalizer = os.environ.get("NORMALIZE_CSV", "normalize_csv")
    jobs = []
    views = {}
    for table, names in INPUT_COLUMNS.items():
        columns = names.split()
        directory = data / table
        sources = sorted(directory.glob("*.csv")) if directory.is_dir() else [data / f"{table}.csv"]
        paths = []
        for index, source in enumerate(sources):
            target = work / f"{table}-{index}.csv"
            paths.append(target)
            jobs.append([normalizer, str(source), str(target), str(len(columns))])
        if paths:
            select = csv_scan(paths, columns)
        else:
            fields = ", ".join(f"NULL::VARCHAR AS {column}" for column in columns)
            select = f"SELECT {fields} WHERE false"
        views[table] = select

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(lambda job: subprocess.run(job, check=True), jobs))
    for table, select in views.items():
        con.execute(f"CREATE VIEW input_{table} AS {select}")


def prepare_people(con, work):
    index = {}
    parents = []
    sizes = []

    def number(user_id):
        if user_id not in index:
            index[user_id] = len(parents)
            parents.append(len(parents))
            sizes.append(1)
        return index[user_id]

    def root(node):
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    rows = con.execute("SELECT * FROM input_links WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL")
    while batch := rows.fetchmany(8192):
        for left, right in batch:
            left, right = root(number(left)), root(number(right))
            if left == right:
                continue
            if sizes[left] < sizes[right]:
                left, right = right, left
            parents[right] = left
            sizes[left] += sizes[right]

    canonical = {}
    for user_id, node in index.items():
        component = root(node)
        # Unicode code-point order is the same as byte order for valid UTF-8.
        canonical[component] = min(user_id, canonical.get(component, user_id))

    path = work / "people.csv"
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("alias", "person"))
        for user_id, node in index.items():
            person = canonical[root(node)]
            if user_id != person:
                writer.writerow((user_id, person))
    con.execute(f"CREATE TABLE people AS {csv_scan([path], ['alias', 'person'], newline=r'\r\n')}")


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    threads = int(os.environ.get("NCPU", "8"))
    memory = int(os.environ.get("MEM_GB", "24"))
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
    scratch.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(dir=scratch) as directory, duckdb.connect() as con:
        work = Path(directory)
        con.execute(f"SET threads = {threads}")
        con.execute(f"SET memory_limit = '{max(1, int(memory * 0.75))}GB'")
        con.execute(f"SET temp_directory = {sql_string(work / 'spill')}")
        con.execute("SET preserve_insertion_order = false")
        con.execute("""
            CREATE TABLE bounds AS SELECT
                epoch_ms(?::TIMESTAMP) AS window_start,
                epoch_ms(?::TIMESTAMP) AS window_end
        """, [manifest["window_start"], manifest["window_end"]])

        prepare_inputs(con, data, work, threads)
        prepare_people(con, work)
        con.execute(Path(__file__).with_name("baseline.sql").read_text(encoding="utf-8"))

        for table in ("user_daily", "campaign_daily"):
            target = output / table / "part-0.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY {table} TO {sql_string(target)} (FORMAT parquet)")


if __name__ == "__main__":
    main()
