# The stock Debezium Connect image extended with the Confluent AVRO converter
# (Confluent Community License). The Block 9 JDBC sink needs it to decode the
# Confluent-framed AVRO payloads PHP writes (magic byte + schema id + binary)
# against cp-schema-registry — the bundled Apicurio converters speak a different
# registry API and wire format. Version pinned to the registry image
# (cp-schema-registry:7.7.1 in compose.yaml).
# OFFLINE NOTE: the curl below needs the internet at BUILD time — build this
# image at home before an offline workshop venue (docker compose build connect).
FROM debezium/connect:3.0.0.Final
ARG CONVERTER_VERSION=7.7.1

# `jar` is not on PATH in this build layer (JAVA_HOME/bin is excluded); use system unzip.
# Each subdirectory of /kafka/connect is one Connect plugin; the loose jars in
# confluent-avro-converter/ become the converter plugin.
RUN mkdir -p /kafka/connect/confluent-avro-converter \
 && cd /kafka/connect/confluent-avro-converter \
 && curl -fsSL -o plugin.zip "https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CONVERTER_VERSION}/archive" \
 && unzip -q plugin.zip \
 && mv "confluentinc-kafka-connect-avro-converter-${CONVERTER_VERSION}/lib/"*.jar . \
 && rm -rf plugin.zip "confluentinc-kafka-connect-avro-converter-${CONVERTER_VERSION}"
