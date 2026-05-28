FROM python:3.11-slim
WORKDIR /app
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker/ .
EXPOSE 8001
CMD ["python", "main.py"]