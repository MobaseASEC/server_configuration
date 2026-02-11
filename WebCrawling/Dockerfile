FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

COPY Source/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

RUN printf "30 8 * * * root python /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 12 * * * root python /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 17 * * * root python /app/crawler.py >> /var/log/crawler.log 2>&1\n" > /etc/cron.d/crawler \
 && chmod 0644 /etc/cron.d/crawler

RUN touch /var/log/crawler.log

CMD ["sh", "-lc", "cron && tail -f /var/log/crawler.log"]