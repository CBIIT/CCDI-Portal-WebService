# Build stage - Oracle JDK 17.0.18
FROM ubuntu:24.04 AS build

# Download and install Oracle JDK 17.0.18
RUN apt-get update && \
    apt-get install -y wget ca-certificates && \
    wget -q https://download.oracle.com/java/17/archive/jdk-17.0.18_linux-x64_bin.tar.gz -O /tmp/jdk.tar.gz && \
    mkdir -p /opt/java && \
    tar -xzf /tmp/jdk.tar.gz -C /opt/java --strip-components=1 && \
    rm /tmp/jdk.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Maven
RUN apt-get update && \
    apt-get install -y wget && \
    wget -q https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz -O /tmp/maven.tar.gz && \
    mkdir -p /opt/maven && \
    tar -xzf /tmp/maven.tar.gz -C /opt/maven --strip-components=1 && \
    rm /tmp/maven.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

RUN java -version && mvn -version

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage - Oracle JDK 17.0.18
FROM tomcat:11.0-jdk17-temurin-noble AS final

# Replace Temurin JDK with Oracle JDK 17.0.18
RUN apt-get update && \
    apt-get install -y wget ca-certificates && \
    wget -q https://download.oracle.com/java/17/archive/jdk-17.0.18_linux-x64_bin.tar.gz -O /tmp/jdk.tar.gz && \
    rm -rf /opt/java/openjdk && \
    mkdir -p /opt/java/openjdk && \
    tar -xzf /tmp/jdk.tar.gz -C /opt/java/openjdk --strip-components=1 && \
    rm /tmp/jdk.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN java -version

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf /usr/local/tomcat/webapps.dist \
           /usr/local/tomcat/webapps/ROOT

RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml

EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war