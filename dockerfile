FROM tiangolo/uwsgi-nginx-flask:python3.8

COPY ./app /app
RUN /usr/local/bin/python -m pip install --upgrade pip

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

ENV TZ Asia/Almaty
