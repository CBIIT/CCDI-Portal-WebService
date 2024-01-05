# Build stage
FROM maven:3.8.5-openjdk-17 as build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage
FROM tomcat:10.1.17-jdk17

RUN apt-get update && apt-get -y upgrade

# install dependencies and clean up unused files
RUN apt-get update && apt-get install unzip
RUN rm -rf /usr/local/tomcat/webapps.dist
RUN rm -rf /usr/local/tomcat/webapps/ROOT

# Create a script to modify the web.xml file
RUN sed -i 's|</web-app>||' /usr/local/tomcat/conf/web.xml \
    && echo '    <error-page>' >> /usr/local/tomcat/conf/web.xml \
    #&& echo '      <exception-type>java.lang.Throwable</exception-type>' >> /usr/local/tomcat/conf/web.xml \
    && echo '      <location>/error.jsp</location>' >> /usr/local/tomcat/conf/web.xml \
    && echo '    </error-page>' >> /usr/local/tomcat/conf/web.xml \
    && echo '</web-app>' >> /usr/local/tomcat/conf/web.xml
COPY --from=build  /usr/src/app/tomcat/conf/error.jsp /usr/local/tomcat/webapps/error.jsp

# expose ports
EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war
