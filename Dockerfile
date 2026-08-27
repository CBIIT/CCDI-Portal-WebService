# Build stage
FROM maven:3.9.9-amazoncorretto-17-al2023 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

FROM maven:3.9.9-amazoncorretto-17-al2023 AS tomcat

ENV CATALINA_HOME=/usr/local/tomcat
# Pinned to latest currently published 11.x archive.
ENV TOMCAT_VERSION=11.0.25

RUN curl -fsSL https://archive.apache.org/dist/tomcat/tomcat-11/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz -o /tmp/tomcat.tar.gz && \
    mkdir -p ${CATALINA_HOME} && \
    tar -xzf /tmp/tomcat.tar.gz -C ${CATALINA_HOME} --strip-components=1 && \
    rm /tmp/tomcat.tar.gz

# Production stage - Amazon Linux 2023 with Corretto 17 and Tomcat 11
FROM amazoncorretto:17-al2023-headless AS final

ENV CATALINA_HOME=/usr/local/tomcat
ENV PATH=$CATALINA_HOME/bin:$PATH
ENV TOMCAT_VERSION=11.0.25

# Cache bust ARG - update this date to force fresh package pulls
# Updated to pull OS patches for CVE-2026-44605 (rpm >= 4.16.1.3-29.amzn2023.0.7),
# CVE-2026-40553/40467/40468 (gawk >= 5.1.0-3.amzn2023.0.4),
# CVE-2026-16118 (glib2 >= 2.82.2-771.amzn2023);
# python3 removed entirely (CVE-2026-15308) - not needed in Java/Tomcat runtime
ARG CACHE_BUST=2026-08-27

# Force refresh repo metadata and install latest security updates
RUN echo "CACHE_BUST=${CACHE_BUST}" && \
    dnf clean all && \
    dnf makecache --refresh && \
    dnf upgrade -y --refresh --best --allowerasing && \
    dnf install -y --setopt=install_weak_deps=False wget unzip graphite2 && \
    dnf upgrade -y --refresh --best --allowerasing rpm gawk glib2 && \
    rpm -q rpm gawk glib2 && \
    dnf clean all && \
    rm -rf /var/cache/dnf && \
    (rpm -e --nodeps python3 python3-libs python3-setuptools-wheel python3-pip-wheel \
        python3-dnf python3-libdnf python3-hawkey python3-rpm python3-gpg \
        python3-libcomps 2>/dev/null || true) && \
    rpm -q --qf '%{NAME} %{VERSION}-%{RELEASE}\n' libcap gnutls openssl-libs openssl-fips-provider-latest graphite2 gnupg2-minimal

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