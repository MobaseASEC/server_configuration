FROM python:3.11-slim

WORKDIR /app

# 필요한 패키지 설치
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# crontab 설정
RUN echo "30 8 * * * python /app/crawler.py >> /var/log/crawler.log 2>&1" > /etc/cron.d/crawler
RUN echo "30 12 * * * python /app/crawler.py >> /var/log/crawler.log 2>&1" >> /etc/cron.d/crawler
RUN echo "30 17 * * * python /app/crawler.py >> /var/log/crawler.log 2>&1" >> /etc/cron.d/crawler
RUN chmod 0644 /etc/cron.d/crawler
RUN crontab /etc/cron.d/crawler

# cron 데몬 시작
CMD ["cron", "-f"]