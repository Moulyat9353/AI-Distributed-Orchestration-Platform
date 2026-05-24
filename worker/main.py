import json
import os
import logging
import redis
from kafka import KafkaConsumer
from processors.summarizer import summarize
from prometheus_client import Counter, start_http_server

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

JOBS_PROCESSED = Counter("worker_jobs_processed_total", "Jobs processed")
JOBS_FAILED    = Counter("worker_jobs_failed_total", "Jobs failed")

redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

def run():
    start_http_server(8001)
    log.info("Worker metrics server started on port 8001")

    consumer = KafkaConsumer(
        "ai-jobs",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="ai-worker-group",
        auto_offset_reset="earliest"
    )

    log.info("Worker started, listening on ai-jobs topic...")

    for message in consumer:
        job   = message.value
        job_id = job["job_id"]
        text   = job["text"]

        log.info(f"Processing job {job_id}")
        try:
            redis_client.hset(f"job:{job_id}", "status", "PROCESSING")
            result = summarize(text)
            redis_client.hset(f"job:{job_id}", mapping={
                "status": "COMPLETED",
                "result": result
            })
            JOBS_PROCESSED.inc()
            log.info(f"Completed job {job_id}")
        except Exception as e:
            redis_client.hset(f"job:{job_id}", mapping={
                "status": "FAILED",
                "result": str(e)
            })
            JOBS_FAILED.inc()
            log.error(f"Failed job {job_id}: {e}")

if __name__ == "__main__":
    run()