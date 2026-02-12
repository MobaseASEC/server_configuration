FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

COPY Source/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

RUN printf "SHELL=/bin/sh\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n\
30 08 * * * root export \$(tr '\\0' '\\n' < /proc/1/environ | grep -E '^MYSQL_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 12 * * * root export \$(tr '\\0' '\\n' < /proc/1/environ | grep -E '^MYSQL_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 17 * * * root export \$(tr '\\0' '\\n' < /proc/1/environ | grep -E '^MYSQL_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n" > /etc/cron.d/crawler \
 && chmod 0644 /etc/cron.d/crawler



RUN touch /var/log/crawler.log

CMD ["sh", "-lc", "touch /var/log/crawler.log && cron -f"]