FROM gradle:8.14.4-jdk8 AS build-image

WORKDIR /app
COPY ./*.gradle ./
COPY ./src/main/java/ru/itmo/hadoop ./src/main/java/ru/itmo/hadoop
RUN gradle buildShadow

# FROM eclipse-temurin:8-jdk

# ARG HADOOP_VERSION=3.2.1

# RUN mkdir -p /opt/hadoop-${HADOOP_VERSION}/{etc/hadoop,lib}
# COPY --from=build-image /app/build/libs/sales-analysis-hadoop-1.0-SNAPSHOT.jar /opt/hadoop-${HADOOP_VERSION}/lib/

# CMD ["hadoop", "jar", "/jars/sales-analysis-hadoop-1.0-SNAPSHOT.jar", "com.example.SalesAnalysisDriver", "/input", "/output", "sum"]