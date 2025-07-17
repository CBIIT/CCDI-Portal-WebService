# === Stage 1: Build WAR file ===
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# === Stage 2: Final Image with Patched JDK + Tomcat 11.0.9 ===
FROM eclipse-temurin:17.0.10_7-jdk AS final

ENV TOMCAT_VERSION=11.0.9
RUN curl -fsSL https://downloads.apache.org/tomcat/tomcat-11/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    | tar xz -C /usr/local && \
    mv /usr/local/apache-tomcat-${TOMCAT_VERSION} /usr/local/tomcat

WORKDIR /usr/local/tomcat

# Clean up default apps
RUN rm -rf webapps.dist webapps/ROOT

# Harden Tomcat
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml

COPY --from=build /usr/src/app/target/Bento-0.0.1.war webapps/ROOT.war

EXPOSE 8080

CMD ["bin/catalina.sh", "run"]

