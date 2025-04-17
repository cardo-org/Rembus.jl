FROM ubuntu:24.04

WORKDIR /broker

COPY --chown=ubuntu:ubuntu build .
RUN chmod -R a+w /broker/share/julia

EXPOSE 8000
EXPOSE 8001
EXPOSE 8002
EXPOSE 9100

USER ubuntu
ENTRYPOINT ["bin/sv", "bin/broker"]


