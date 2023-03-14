FROM python:3.9-slim-buster

WORKDIR /home

COPY requirements.txt requirements.txt
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

COPY . .
# 환경변수 선언 연습
ENV UPBIT_API_SERVER https://api.upbit.com 
