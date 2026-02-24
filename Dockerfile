# Build stage
FROM maven:3.9.9-amazoncorretto-17-al2023 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage - Amazon Linux 2023 with Corretto 17 and Tomcat 11
FROM amazoncorretto:17-al2023 AS final

ENV CATALINA_HOME=/usr/local/tomcat
ENV PATH=$CATALINA_HOME/bin:$PATH
ENV TOMCAT_VERSION=11.0.12

RUN dnf update -y && \
    dnf install -y unzip tar gzip shadow-utils wget && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Download and install Tomcat 11
RUN curl -fsSL https://archive.apache.org/dist/tomcat/tomcat-11/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz -o /tmp/tomcat.tar.gz && \
    mkdir -p ${CATALINA_HOME} && \
    tar -xzf /tmp/tomcat.tar.gz -C ${CATALINA_HOME} --strip-components=1 && \
    rm /tmp/tomcat.tar.gz

RUN rm -rf ${CATALINA_HOME}/webapps.dist \
           ${CATALINA_HOME}/webapps/ROOT \
           ${CATALINA_HOME}/webapps/docs \
           ${CATALINA_HOME}/webapps/examples \
           ${CATALINA_HOME}/webapps/host-manager \
           ${CATALINA_HOME}/webapps/manager

# Security hardening - hide server info in error pages
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' ${CATALINA_HOME}/conf/server.xml

WORKDIR ${CATALINA_HOME}

EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war ${CATALINA_HOME}/webapps/ROOT.war

CMD ["catalina.sh", "run"]