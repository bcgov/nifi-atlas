FROM nifi-node

ADD bootstrap.conf $NIFI_HOME/conf/bootstrap.conf

WORKDIR $NIFI_HOME

RUN curl -O http://dl.zeroturnaround.com/jrebel-stable-nosetup.zip

RUN unzip jrebel-stable-nosetup.zip

EXPOSE 5010 5011

