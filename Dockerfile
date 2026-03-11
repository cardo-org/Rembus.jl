FROM ubuntu:24.04

WORKDIR /broker

COPY --chown=ubuntu:ubuntu build .
COPY --chown=ubuntu:ubuntu ./bin/init_keystore ./bin/init_keystore

RUN chmod -R a+w ./share/julia
RUN chmod +x ./bin/init_keystore

EXPOSE 8338
EXPOSE 8337
EXPOSE 8336
EXPOSE 9100

USER ubuntu
ENTRYPOINT ["bin/sv", "bin/broker"]


