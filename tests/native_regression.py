"""Check native chunk boundaries and multiword assists through the scorer interface."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import duckdb


def main(command):
    with TemporaryDirectory() as directory:
        root = Path(directory)
        data = root / "data"
        for name in ("events", "clicks", "links"):
            (data / name).mkdir(parents=True)
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=14)

        def f1(time):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ")

        def stamp(i):
            return start + timedelta(hours=i * 2)

        (data / "manifest.json").write_text(json.dumps({"window_start": f1(start), "window_end": f1(end)}))
        (data / "products.csv").write_bytes(b"product_id,category\np1,books\n")
        (data / "campaigns.csv").write_bytes(b"campaign_id,channel\na,search\nb,search\nc,social\nd,social\n")
        (data / "links" / "part-0.csv").write_bytes(b"user_id_a,user_id_b\na-person-with-a-long-id,alias\n")

        header = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
        malformed = b"x" * (4 * 1024 * 1024 - len(header) - 11) + b"\n"
        events = []
        for i in range(130):
            ts = f1(stamp(i))
            kind, price = ("purchase", "100") if i == 64 else ("view", "")
            events.append(f"e{i},alias,{ts},{ts},{kind},p1,{price}")
        events.extend((
            f"broken,alias,{f1(stamp(3))},{f1(stamp(3))},purchase,p1,100\r",
            f"broken,alias,{f1(stamp(3))},{f1(stamp(4))},purchase,p1,100",
        ))
        # More shoppers than partitions guarantees reuse of bucket state in a worker.
        for shopper in range(129):
            for i in range(64):
                kind, price = ("purchase", "1") if i == 0 else ("view", "")
                ts = f1(stamp(64))
                events.append(f"reuse{shopper}-{i},reuse{shopper},{ts},{ts},{kind},p1,{price}")
        (data / "events" / "part-0.csv").write_bytes(header + malformed + "\n".join(events).encode())

        clicks = [
            ("click-a", "a", 0), ("click-next", "a", 60), ("click-next", "a", 60),
            ("zzzz-a-very-long-click-id", "c", 64), ("aaaa-a-very-long-click-id", "b", 64),
            ("zzz-first", "d", 0),
        ]
        records = [f"{click},alias,{campaign},{f1(stamp(i))}\n" for click, campaign, i in clicks]
        (data / "clicks" / "part-0.csv").write_bytes(("click_id,user_id,campaign_id,ts\n" + "".join(records)).encode())

        out = root / "output"
        subprocess.run(command, env=dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), NCPU="4"), check=True)
        with duckdb.connect() as db:
            user = db.execute("""
                SELECT min(user_id), max(user_id), sum(sessions), sum(events), sum(revenue_cents), max(top_category)
                FROM read_parquet(?) WHERE user_id = 'a-person-with-a-long-id'
            """, [str(out / "user_daily" / "*.parquet")]).fetchone()
            assert user == ("a-person-with-a-long-id", "a-person-with-a-long-id", 130, 130, 100, "books"), user
            reused = db.execute("""
                SELECT count(*), count(*) FILTER (WHERE sessions = 1 AND events = 64 AND revenue_cents = 1)
                FROM read_parquet(?) WHERE starts_with(user_id, 'reuse')
            """, [str(out / "user_daily" / "*.parquet")]).fetchone()
            assert reused == (129, 129), reused
            campaign = db.execute("""
                SELECT campaign_id, sum(sessions), sum(purchasing_sessions), sum(revenue_cents), sum(assisted_sessions)
                FROM read_parquet(?) GROUP BY campaign_id ORDER BY campaign_id
            """, [str(out / "campaign_daily" / "*.parquet")]).fetchall()
            assert campaign == [
                ("a", 64, 0, 0, 130), ("b", 66, 1, 100, 66), ("c", 0, 0, 0, 66),
                ("d", 0, 0, 0, 85), ("organic", 129, 129, 129, 0),
            ], campaign


if __name__ == "__main__":
    main(sys.argv[1:])
