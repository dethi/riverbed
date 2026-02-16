FROM maven:3.9-eclipse-temurin-11

# Install Hadoop native libs (includes libzstd support).
ARG HADOOP_VERSION=3.4.2
ADD https://dlcdn.apache.org/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz /tmp/
RUN tar --strip-components=1 -C /usr/local -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
      hadoop-${HADOOP_VERSION}/lib/native && \
    rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz
ENV LD_LIBRARY_PATH=/usr/local/lib/native

WORKDIR /gen
COPY pom.xml .
# Pre-download dependencies so rebuilds after source changes are fast.
RUN mvn -q dependency:go-offline
COPY src src

# Remove config to force the use of hadoop-native
RUN rm -f src/main/resources/hbase-site.xml

# Default: compile once, then accept JSON configs on stdin.
CMD ["mvn", "-q", "compile", "exec:java", "-Dexec.mainClass=GenerateHFileServer"]
