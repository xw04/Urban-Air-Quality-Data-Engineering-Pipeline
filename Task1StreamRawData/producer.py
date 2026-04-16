# Author: Yaw Wei Ying

import argparse, time, signal, sys, json, csv, subprocess
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

class KafkaCSVProducer:
    def __init__(self, bootstrap, topic, file, hdfs_path, delimiter=",",
                 has_header=False, delay=0.5, compression="lz4",
                 log_every=1, show_metadata=False):
        self.bootstrap = bootstrap
        self.topic = topic
        self.file = file
        self.hdfs_path = hdfs_path
        self.delimiter = delimiter
        self.has_header = has_header
        self.delay = delay
        self.compression = None if compression == "none" else compression
        self.log_every = log_every
        self.show_metadata = show_metadata
        self.running = True
        self.producer = None

    def connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                acks="all",
                enable_idempotence=True,
                compression_type=self.compression,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                linger_ms=10,
                retries=5,
            )
        except NoBrokersAvailable:
            print("ERROR: No brokers available.", file=sys.stderr)
            sys.exit(1)

    def stop(self, *_):
        self.running = False

    def hdfs_append(self, line: str):
        try:
            tmp_path = "/tmp/hdfs_buf.json"
            with open(tmp_path, "w", encoding="utf-8") as tmp:
                tmp.write(line + "\n")
            subprocess.run(
                ["hdfs", "dfs", "-appendToFile", tmp_path, self.hdfs_path],
                check=True, capture_output=True, text=True
            )
        except Exception as e:
            print(f"[WARN] Could not write to HDFS: {e}", file=sys.stderr)

    def on_send_success(self, record_metadata, payload_json):
        if self.log_every > 0:
            print(payload_json, flush=True)
            if self.show_metadata:
                print(f"  ↳ {record_metadata.topic}-{record_metadata.partition}@{record_metadata.offset}",
                      flush=True)

    def on_send_error(self, excp, payload_json):
        print(f"[ERROR] send failed for row: {payload_json} :: {excp}", file=sys.stderr, flush=True)

    def run(self):
        self.connect()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        count = 0
        with open(self.file, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            header = next(reader, None) if self.has_header else None
            if header:
                header = [h.strip("\ufeff").strip() for h in header]

            for row in reader:
                if not self.running:
                    break

                if header:
                    record = {header[i]: row[i] if i < len(row) else None for i in range(len(header))}
                else:
                    record = {f"c{i}": v for i, v in enumerate(row)}

                record_json = json.dumps(record, ensure_ascii=False)

                fut = self.producer.send(self.topic, value=record)
                if self.log_every > 0 and (count % self.log_every == 0):
                    fut.add_callback(self.on_send_success, record_json).add_errback(self.on_send_error, record_json)
                else:
                    fut.add_errback(self.on_send_error, record_json)

                self.hdfs_append(record_json)

                count += 1
                if self.delay:
                    time.sleep(self.delay)

        self.producer.flush()
        self.producer.close()
        print(f"done. total sent: {count}")

def parse_args():
    p = argparse.ArgumentParser(description="CSV → Kafka + HDFS producer")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--hdfs-path", required=True)
    p.add_argument("--delimiter", default=",")
    p.add_argument("--has-header", action="store_true")
    p.add_argument("--delay", type=float, default=0.5)
    p.add_argument("--compression", default="lz4")
    p.add_argument("--log-every", type=int, default=1)
    p.add_argument("--show-metadata", action="store_true")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    app = KafkaCSVProducer(**vars(args))
    app.run()
