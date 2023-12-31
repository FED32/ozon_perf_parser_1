FROM python:3

WORKDIR ./app
COPY . .
RUN chmod 775 script.sh

RUN pip install --upgrade pip && pip install --no-cache-dir --upgrade -r requirements.txt

RUN apt update
RUN apt install -y wget
RUN mkdir -p ~/.postgresql && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O ~/.postgresql/root.crt && \
    chmod 0600 ~/.postgresql/root.crt
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/ && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
    --output-document /usr/local/share/ca-certificates/Yandex/YandexCA.crt && \
    chmod 655 /usr/local/share/ca-certificates/Yandex/YandexCA.crt

