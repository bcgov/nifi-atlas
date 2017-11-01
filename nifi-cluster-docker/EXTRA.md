# Building NIFI Atlas Bridge

`````
git clone -b nifi-3709-2 https://github.com/ikethecoder/nifi.git

docker pull maven:3.5.0-jdk-8

docker run -v `pwd`/nifi:/source -w="/source" maven:3.5.0-jdk-8  mvn -DskipTests install
`````

Be patient, it will take about 15 minutes to run.




docker run -t -i maven:3.5.0-jdk-8 bash

