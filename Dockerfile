FROM python:3.7

COPY AdaWPBot.py /bot/
COPY Constants.py /bot/
COPY css.py /bot/
COPY .env /bot/
COPY requirements.txt /tmp
WORKDIR /bot
#RUN apk --update add python py-pip openssl ca-certificates py-openssl wget
#RUN apk --update add --virtual build-dependencies libffi-dev openssl-dev python-dev py-pip build-base \
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt
RUN apt-get update
RUN apt-get -y install cron
ADD Metabase-WP-Update /etc/cron.d/cron_jobs
RUN chmod 0644 /etc/cron.d/cron_jobs
RUN touch /var/log/cron.log
RUN /usr/bin/crontab /etc/cron.d/cron_jobs

CMD printenv | grep -v "no_proxy" >> /etc/environment && cron && tail -f /var/log/cron.log