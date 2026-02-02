# Build stage - Amazon Corretto JDK 17
FROM maven:3.9.9-amazoncorretto-17 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage - Amazon Corretto JDK 17
FROM amazoncorretto:17 AS final

# Install Tomcat 11
RUN yum install -y tar gzip shadow-utils && \
    yum clean all

ENV CATALINA_HOME=/usr/local/tomcat
ENV PATH="${CATALINA_HOME}/bin:${PATH}"

RUN curl -fsSL https://archive.apache.org/dist/tomcat/tomcat-11/v11.0.2/bin/apache-tomcat-11.0.2.tar.gz | tar -xz -C /usr/local && \
    mv /usr/local/apache-tomcat-11.0.2 ${CATALINA_HOME} && \
    rm -rf ${CATALINA_HOME}/webapps.dist ${CATALINA_HOME}/webapps/ROOT

# Security hardening - hide server info in error pages
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' ${CATALINA_HOME}/conf/server.xml

WORKDIR ${CATALINA_HOME}

EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war ${CATALINA_HOME}/webapps/ROOT.war

CMD ["catalina.sh", "run"]