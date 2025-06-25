# Build stage
FROM maven:3.8.5-openjdk-17 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage
FROM tomcat:10.1.14-jdk17-temurin AS final

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

# Optional: Add a non-root user (basic hardening)
RUN groupadd -r appgroup && useradd -r -g appgroup appuser
USER appuser

EXPOSE 8080

# Deploy WAR
COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war
