FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV PORT=8080
CMD ["python","-m","functions_framework","--target=docai_runner","--port=8080"]
