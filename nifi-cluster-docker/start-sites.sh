#!/bin/bash

docker-compose -f docker-compose-site-a.yml -p nificluster_a up -d
docker-compose -f docker-compose-site-b.yml -p nificluster_b up -d

