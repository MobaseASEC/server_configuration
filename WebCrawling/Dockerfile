FROM python:3.11-slim

WORKDIR /app

# cron + timezone(KST) 고정
RUN apt-get update && apt-get install -y cron tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


COPY Source/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# cron 스케줄 등록 (08:30 / 12:30 / 17:30, KST)
RUN printf "SHELL=/bin/sh\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n\
CRON_TZ=Asia/Seoul\n\
30 8  * * * root export \$(tr '\\0' '\\n' </proc/1/environ | grep -E '^(MYSQL|SLACK|NEWS)_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 12 * * * root export \$(tr '\\0' '\\n' </proc/1/environ | grep -E '^(MYSQL|SLACK|NEWS)_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n\
30 17 * * * root export \$(tr '\\0' '\\n' </proc/1/environ | grep -E '^(MYSQL|SLACK|NEWS)_' | xargs) && /usr/local/bin/python3 /app/crawler.py >> /var/log/crawler.log 2>&1\n" > /etc/cron.d/crawler \
 && chmod 0644 /etc/cron.d/crawler

# 로그 파일 준비
RUN touch /var/log/crawler.log

# cron 실행 + 로그를 docker logs로 확인
CMD ["sh", "-lc", "cron && tail -f /var/log/crawler.log"]
