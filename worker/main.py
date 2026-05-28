import json
import os
import logging
import redis
from kafka import KafkaConsumer
from processors.summarizer import summarize, analyze_sentiment, extract_keywords
from prometheus_client import Counter, start_http_server

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

JOBS_PROCESSED = Counter("worker_jobs_processed_total", "Jobs processed", ["task_type"])
JOBS_FAILED    = Counter("worker_jobs_failed_total", "Jobs failed", ["task_type"])

redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"))

def process(task_type: str, text: str) -> str:
    if task_type == "summarize":
        return summarize(text)
    elif task_type == "sentiment":
        return analyze_sentiment(text)
    elif task_type == "keywords":
        return extract_keywords(text)
    else:
        raise ValueError(f"Unknown task type: {task_type}")

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
    log.info("Kafka consumer initialized successfully")
    log.info("Worker started, listening on ai-jobs topic...")

    for message in consumer:
        try:
            log.info(f"Received raw Kafka message: {message}")

            job = message.value
            log.info(f"Decoded message value: {job}")

            job_id = job["job_id"]
            text = job["text"]
            task_type = job.get("task_type", "summarize")

            log.info(f"Processing job {job_id} | task: {task_type}")

            redis_client.hset(f"job:{job_id}", "status", "PROCESSING")

            result = process(task_type, text)

            redis_client.hset(f"job:{job_id}", mapping={
                "status": "COMPLETED",
                "result": result
            })

            JOBS_PROCESSED.labels(task_type=task_type).inc()

            log.info(f"Completed job {job_id} | task: {task_type}")

        except Exception as e:
            JOBS_FAILED.labels(task_type="unknown").inc()
            log.exception(f"Failed processing Kafka message: {e}")

if __name__ == "__main__":
    run()