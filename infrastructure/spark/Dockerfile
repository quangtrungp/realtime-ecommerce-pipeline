# ------------------------------------------------------------
# Stage 1: Resolve Kafka connector and its transitive dependencies
# ------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21 AS kafka-dependencies

ARG SPARK_VERSION=4.2.0

WORKDIR /build

RUN cat > pom.xml <<'POM_END'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>local.spark</groupId>
    <artifactId>spark-kafka-runtime</artifactId>
    <version>1.0.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.13</artifactId>
            <version>${SPARK_VERSION}</version>
        </dependency>
    </dependencies>
</project>
POM_END

RUN mvn -B -f pom.xml -DSPARK_VERSION=${SPARK_VERSION} dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/kafka-jars

# ------------------------------------------------------------
# Stage 2: Download existing storage and database dependencies
# ------------------------------------------------------------
FROM alpine:3.22 AS storage-dependencies

ARG HADOOP_VERSION=3.5.0
ARG AWS_SDK_VERSION=2.35.4
ARG S3_ANALYTICS_ACCELERATOR_VERSION=1.3.1
ARG POSTGRES_JDBC_VERSION=42.7.13

RUN apk add --no-cache wget

RUN wget -O /hadoop-aws.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
 && wget -O /aws-sdk-bundle.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
 && wget -O /analyticsaccelerator-s3.jar \
    "https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${S3_ANALYTICS_ACCELERATOR_VERSION}.jar" \
 && wget -O /postgresql-jdbc.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# ------------------------------------------------------------
# Stage 3: Final Spark image shared by Master and Workers
# ------------------------------------------------------------
FROM spark:4.2.0-python3

USER root

# Existing MinIO/S3 and PostgreSQL dependencies
COPY --from=storage-dependencies /hadoop-aws.jar /opt/spark/jars/hadoop-aws.jar
COPY --from=storage-dependencies /aws-sdk-bundle.jar /opt/spark/jars/aws-sdk-bundle.jar
COPY --from=storage-dependencies /analyticsaccelerator-s3.jar /opt/spark/jars/analyticsaccelerator-s3.jar
COPY --from=storage-dependencies /postgresql-jdbc.jar /opt/spark/jars/postgresql-jdbc.jar

# Kafka connector plus its runtime dependencies
COPY --from=kafka-dependencies /kafka-jars/ /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

USER 185
