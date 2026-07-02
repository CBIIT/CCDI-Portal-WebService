# Build stage
FROM maven:3.9.9-amazoncorretto-17-al2023 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

FROM maven:3.9.9-amazoncorretto-17-al2023 AS tomcat

ENV CATALINA_HOME=/usr/local/tomcat
ENV TOMCAT_VERSION=11.0.23

RUN curl -fsSL https://archive.apache.org/dist/tomcat/tomcat-11/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz -o /tmp/tomcat.tar.gz && \
    mkdir -p ${CATALINA_HOME} && \
    tar -xzf /tmp/tomcat.tar.gz -C ${CATALINA_HOME} --strip-components=1 && \
    rm /tmp/tomcat.tar.gz

# Production stage - Amazon Linux 2023 with Corretto 17 and Tomcat 11
FROM amazoncorretto:17-al2023-headless AS final

ENV CATALINA_HOME=/usr/local/tomcat
ENV PATH=$CATALINA_HOME/bin:$PATH
ENV TOMCAT_VERSION=11.0.23

# Cache bust ARG - update this date to force fresh package pulls
ARG CACHE_BUST=2026-06-30

# Force refresh repo metadata and install fixed package versions
RUN echo "CACHE_BUST=${CACHE_BUST}" && \
    dnf clean all && \
    dnf makecache --refresh && \
    dnf upgrade -y --refresh --best --allowerasing && \
    dnf install -y --setopt=install_weak_deps=False wget unzip graphite2 python3-pip-wheel && \
    dnf install -y --refresh --best \
        'libcap >= 0:2.73-1.amzn2023.0.7' \
        'gnutls >= 0:3.8.3-8.amzn2023.0.3' && \
    rpm -q --qf '%{NAME} %{VERSION}-%{RELEASE}\n' libcap gnutls openssl-libs openssl-fips-provider-latest graphite2 python3-pip-wheel gnupg2-minimal && \
    dnf clean all && \
    rm -rf /var/cache/dnf

COPY --from=tomcat /usr/local/tomcat ${CATALINA_HOME}

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