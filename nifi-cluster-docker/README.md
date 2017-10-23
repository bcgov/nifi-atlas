


# Architecture

- Two NIFI Clusters running in docker containers

Site A: http://nifi.demo:5080/nifi
Site B: http://nifi.demo:6080/nifi

# Building NIFI Cluster

Set DOCKER_HOSTADDR to the IP address of the host that will be running the NIFI clusters.  Needed for Site to Site.

Set HADOOP_ADDR to the location of the HDFS environment.  Needed for HDFS processors.

    export DOCKER_HOSTADDR=174.138.41.167

    export HADOOP_ADDR=174.138.41.167

    docker-compose -f docker-compose-site-a.yml -p nificluster_a build
    docker-compose -f docker-compose-site-b.yml -p nificluster_b build

Note: For more complicated hdfs configuration, the hdfs-site.xml file can be overwritten by creating a new Dockerfile and including:
    ADD ./hdfs-site.xml $NIFI_HOME/conf

# Running the NIFI clusters

## Preparing HDFS Folders

A root folder on HDFS should be setup for NIFI:
- /user/nifi

su - hadoop -c "(export JAVA_HOME=/usr/java/latest && cd /opt/hadoop-2.8.1 && ./bin/hdfs dfs -mkdir /user/nifi)"

su - hadoop -c "(export JAVA_HOME=/usr/java/latest && cd /opt/hadoop-2.8.1 && ./bin/hdfs dfs -chown -R nifi: /user/nifi)"

The following folders will get created by the sample scenarios:

Scenario 3:
- /user/nifi/backup

Scenario 4:
- /user/nifi/inbox

Scenario 5:
- /user/nifi/site_b_inbound

Scenario 6:
- /user/nifi/receiver
- /user/nifi/scenario6/out

## Preparing NIFI-DATA Folders

    mkdir -p nifi-data/a/1/conf nifi-data/a/2/conf nifi-data/a/files
    mkdir -p nifi-data/b/1/conf nifi-data/b/2/conf nifi-data/b/files
    cp flow-a.xml.gz nifi-data/a/1/conf/flow.xml.gz
    cp flow-a.xml.gz nifi-data/a/2/conf/flow.xml.gz
    cp flow-b.xml.gz nifi-data/b/1/conf/flow.xml.gz
    cp flow-b.xml.gz nifi-data/b/2/conf/flow.xml.gz

    mkdir -p nifi-data/a/files/scenario1/landing
    mkdir -p nifi-data/a/files/scenario2/landing
    mkdir -p nifi-data/a/files/scenario3/landing
    mkdir -p nifi-data/a/files/scenario4/landing
    mkdir -p nifi-data/a/files/scenario5/landing

    chown -R hadoop:docker nifi-data

    NOTE: The NIFI user running in the container has UID = 1000.  So the owner of nifi/data must be the user with UID 1000 on the host server.

## Running

    docker-compose -f docker-compose-site-a.yml -p nificluster_a up -d
    docker-compose -f docker-compose-site-b.yml -p nificluster_b up -d

## Verifying a particular instance

You can check the logs by executing bash shell within the particular container:

    docker-compose -f docker-compose-site-a.yml -p nificluster_a exec nifi-node-1 bash
    docker-compose -f docker-compose-site-a.yml -p nificluster_a exec nifi-node-2 bash

    docker-compose -f docker-compose-site-b.yml -p nificluster_b exec nifi-node-1 bash
    docker-compose -f docker-compose-site-b.yml -p nificluster_b exec nifi-node-2 bash


## Running the scenario tests:

### Scenario 1

cp sample-files/sample_1 nifi-data/a/files/scenario1/landing/data_1

Expected result:
    - File written to: ./nifi-data/a/files/scenario1/output/data_1

### Scenario 2

cp sample-files/sample_1 nifi-data/a/files/scenario2/landing/data_2

Expected result:
    - File written to: ./nifi-data/a/files/scenario2/output/data_2.out

### Scenario 3

cp sample-files/sample_1 nifi-data/a/files/scenario3/landing/data_3

Expected result:
    - File written to: ./nifi-data/a/files/scenario3/backup/data_3
    - File written to: hdfs : /user/nifi/backup

### Scenario 4

cp sample-files/sample_1 nifi-data/a/files/scenario4/landing/data_4

Expected result:
    - File written to: ./nifi-data/a/files/scenario4/inbox/data_4
    - File written to: hdfs : /user/nifi/inbox

### Scenario 5

cp sample-files/sample_1 nifi-data/a/files/scenario5/landing/data_5

Expected result:
    - File written to: hdfs : /user/nifi/site_b_inbound

### Scenario 6

Site B has a daily scheduled job to call: wwww.google.com.

Expected result:
    - File written to: ./nifi-data/a/files/scenario6/out/home_google.html.gz
    - File written to: hdfs : /user/nifi/scenario6/out
    - File written to: hdfs : /user/nifi/receiver



