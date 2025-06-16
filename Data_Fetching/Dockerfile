FROM python:3.10-slim

WORKDIR /app

#S3 bucket (at least from Kopah) requirements
ENV AWS_REQUEST_CHECKSUM_CALCULATION=when_required
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=when_required

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY FRED_crawler.py .

CMD ["python", "FRED_crawler.py"]
