# Build stage - reverted to Java 17
FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage - Java 17
FROM tomcat:11.0.12-jdk17-temurin-noble AS final

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