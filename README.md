# Checkita Data Quality

**Latest Version: 1.0.0**

**Checkita** is a data quality framework written is Scala 2.12 and uses Spark 2.4+ as a computation core.
This framework is used to perform parallel and distributed quality checks on big data environments.
Its functionality allows calculating various metrics over the large datasets and perform multiple checks to ensure quality of these datasets.

For more information, please see the [Documentation](./docs/en/index.md) pages.

## Project Build

Project is built using SBT. There are several JVM variables available to make builds more flexible:
* `-DPSARK_VERSION` - version of spark to build for. (support spark versions are: 2.4.x and 3.x.x);
* `-DPKG_TYPE` - type of the package to build: `SNAPSHOT`, `DEV`, `RELEASE`.
  Also available release candidate types: `RC1` - `RC5`.
* `-DPUBLISH_REALM` and `-DPUBLISH_URL` - type of the repository to publish package and its url.
  By default, package is published locally.
* `-DASSY_MODE` - mode used to assemble uber-jars. `WITHSPARK` and `NOSPARK` options are available.
  Accordingly, these options define whether to include spark dependencies in uber-jar or not.
  Often, when running application in cluster all the spark dependencies are already available and there
  is no need to include them in **Checkita** uber-jar.

## Contribution

Thank you for considering contributing to our project! We welcome contributions from everyone. By participating in 
this project, you agree to abide by our [Code of Conduct](docs/code-of-conduct.md).

Please take a moment to review our [Contribution guide](docs/contribution.md) in order to make the contribution process as smooth as possible.

## License

Checkita Data Quality is [GNU LGPL](LICENSE.txt) licensed.

This project is a reimagination of [Data Quality Framework](https://github.com/agile-lab-dev/DataQuality) developed by Agile Lab, Italy.