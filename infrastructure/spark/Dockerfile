FROM alpine:3.22 AS dependencies

ARG HADOOP_VERSION=3.5.0
ARG AWS_SDK_VERSION=2.35.4
ARG S3_ANALYTICS_ACCELERATOR_VERSION=1.3.1
ARG POSTGRES_JDBC_VERSION=42.7.13

RUN wget \
    -O /hadoop-aws.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"

RUN wget \
    -O /aws-sdk-bundle.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"

RUN wget \
    -O /analyticsaccelerator-s3.jar \
    "https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${S3_ANALYTICS_ACCELERATOR_VERSION}.jar"

RUN wget \
    -O /postgresql-jdbc.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"


FROM spark:4.2.0-python3

USER root

COPY --from=dependencies \
    /hadoop-aws.jar \
    /opt/spark/jars/hadoop-aws.jar

COPY --from=dependencies \
    /aws-sdk-bundle.jar \
    /opt/spark/jars/aws-sdk-bundle.jar

COPY --from=dependencies \
    /analyticsaccelerator-s3.jar \
    /opt/spark/jars/analyticsaccelerator-s3.jar

COPY --from=dependencies \
    /postgresql-jdbc.jar \
    /opt/spark/jars/postgresql-jdbc.jar

RUN chmod 644 \
    /opt/spark/jars/hadoop-aws.jar \
    /opt/spark/jars/aws-sdk-bundle.jar \
    /opt/spark/jars/analyticsaccelerator-s3.jar \
    /opt/spark/jars/postgresql-jdbc.jar

USER 185

