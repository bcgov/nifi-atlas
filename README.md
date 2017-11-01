# nifi-atlas
A bridge to Apache Atlas for provenance metadata created from  data transformations completed by Apache NiFi .

## Getting Started

1. Get your local maven repository populated with all the dependent JAR files related to NiFi ver. 1.4.0

```
git clone https://github.com/apache/nifi.git

cd nifi

git checkout tags/rel/nifi-1.4.0

mvn install -DskipTests=true
```

There are also two projects that were part of NiFi, which we need to include:

- nifi-nar-bundles/nifi-framework-bundle/nifi-client-dto
- nifi-nar-bundles/nifi-extension-utils/nifi-reporting-utils


```
git clone https://github.com/apache/nifi.git
```


Be patient, it will take about 15 minutes to run.

2. Build the new nifi-atlas bundle

```
cd nifi-atlas/nifi-atlas-bundle

mvn install
```

3. Build the dual-site nifi-cluster

Copy over the newly built .nar file from: nifi-atlas-bundle

```
cp ./nifi-atlas-bundle/nifi-atlas-nar/target/nifi-atlas-nar-1.5.0-SNAPSHOT.nar ./nifi-cluster-docker/nifi-node/.

```

The reminder of the setup can be followed in the [nifi-cluster-docker README](nifi-cluster-docker/README.md).


### Prerequisites 

• Java 8
• Apache Atlas 0.8+
• Apache NiFi 1.4+
• Apache Kafka 0.10+

### How to Contribute

If you would like to contribute to this project, please see our [CONTRIBUTING](CONTRIBUTING.md) guidelines.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.


## License

This project is licensed under the Apache Version 2 License- see the [LICENSE.md](LICENSE.md) file for details





