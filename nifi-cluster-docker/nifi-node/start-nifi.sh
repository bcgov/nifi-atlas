#!/bin/sh

echo "Starting as "`id`

ls -l /var/data

cd $NIFI_HOME

cat conf/nifi.properties.templ | bin/mo > conf/nifi.properties

sed -i -e \
  "s|^nifi.web.http.host=.*$|nifi.web.http.host=$(hostname)|" \
  conf/nifi.properties
sed -i -e \
  "s|^nifi.cluster.node.address=.*$|nifi.cluster.node.address=$(hostname)|" \
  conf/nifi.properties
# sed -i -e \
#   "s|^nifi.remote.input.host=.*$|nifi.remote.input.host=$(hostname)|" \
#   conf/nifi.properties

echo $hostname

./bin/nifi.sh run

cat logs/*

