FROM eclipse-temurin:17.0.16_0-jdk AS build

# Download Tomcat 11.0.9
ENV TOMCAT_VERSION=11.0.9
RUN curl -fsSL https://downloads.apache.org/tomcat/tomcat-11/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz | tar xz -C /usr/local \
    && mv /usr/local/apache-tomcat-${TOMCAT_VERSION} /usr/local/tomcat

WORKDIR /usr/local/tomcat

# Clean up default apps
RUN rm -rf webapps.dist webapps/ROOT

EXPOSE 8080

# Harden Tomcat
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml

# Copy WAR from build stage
COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war

CMD ["bin/catalina.sh", "run"]
