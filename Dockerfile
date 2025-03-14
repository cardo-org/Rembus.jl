FROM julia:1.10.2

WORKDIR /broker

COPY build .

EXPOSE 8000
EXPOSE 8001
EXPOSE 8002

ENV BROKER_DIR="/db"

ENTRYPOINT ["bin/sv", "bin/broker"]


