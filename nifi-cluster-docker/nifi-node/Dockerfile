FROM apache/nifi:1.4.0
ADD ./nifi.properties.templ $NIFI_HOME/conf
ADD ./start-nifi.sh $NIFI_HOME/bin
ADD ./nifi-atlas-nar-1.4.0-SNAPSHOT.nar $NIFI_HOME/lib
ADD ./nifi-hive-nar-1.4.0-SNAPSHOT.nar $NIFI_HOME/lib

ADD ./hdfs-site.xml $NIFI_HOME/conf

ADD ./bootstrap.conf $NIFI_HOME/conf
ADD ./logback.xml $NIFI_HOME/conf
ADD ./atlas-application.properties $NIFI_HOME/conf

RUN curl -sSO https://raw.githubusercontent.com/tests-always-included/mo/master/mo

# Make executable
RUN chmod +x mo

# Move to the right folder
RUN mv mo bin/

EXPOSE 8080 8081 8082 9001
ENTRYPOINT ["bin/start-nifi.sh"]

# CMD ["./start-nifi.sh"]
