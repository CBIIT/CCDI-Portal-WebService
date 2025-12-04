# Build stage - Updated to Java 21
FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage - Updated to Java 21
# SECURITY FIX: Updated to 11.0.12 to fix CVE-2025-55754 (CRITICAL), CVE-2025-55752 (HIGH), CVE-2025-61795 (MEDIUM)
FROM tomcat:11.0.12-jdk21-temurin-noble AS final

# Update and install required packages, then clean up
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Clean up default apps
RUN rm -rf /usr/local/tomcat/webapps.dist \
           /usr/local/tomcat/webapps/ROOT

# Harden: disable verbose error and server info in responses
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml

EXPOSE 8080

# Deploy WAR
COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war