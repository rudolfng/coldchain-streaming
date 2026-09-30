ARG BEAM_VERSION=2.74.0
ARG FLINK_VERSION=1.19.3
FROM apache/beam_java17_sdk:${BEAM_VERSION} AS beam-java
FROM apache/beam_python3.12_sdk:${BEAM_VERSION} AS beam-python
FROM flink:${FLINK_VERSION}-java17

USER root
COPY --from=beam-java /opt/apache/beam /opt/apache/beam
COPY --from=beam-python /usr/local /usr/local
COPY --from=beam-python /opt/apache/beam /opt/coldchain-lab/python-sdk
COPY src /opt/coldchain-lab/src
ENV PYTHONPATH=/opt/coldchain-lab/src \
    RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1
# COPY conserva los permisos del host: sin a+rX, un checkout con umask restrictivo deja src
# ilegible para el usuario flink (ModuleNotFoundError en el SDK harness).
RUN chmod 755 /opt/apache/beam/boot /opt/coldchain-lab/python-sdk/boot \
    && chmod -R a+rX /opt/coldchain-lab/src
USER flink
