FROM alpine as builder

RUN apk add --no-cache git build-base
WORKDIR /app
RUN git clone https://github.com/alexandzors/covid-19.git

FROM python:3-alpine
WORKDIR /usr/src/app

COPY --from=builder /app/covid-19/requirements.txt /usr/src/app
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=builder /app/covid-19/covid19.py /usr/src/app

CMD [ "python", "./covid19.py"]