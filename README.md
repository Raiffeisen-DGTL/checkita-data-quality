# Checkita Data Quality

**Latest Version: 1.7.2**

**Checkita** is a data quality framework written is Scala 2.12 and uses Spark 2.4+ as a computation core.
This framework is used to perform parallel and distributed quality checks on big data environments.
Its functionality allows calculating various metrics over the large datasets and perform multiple checks to ensure quality of these datasets.

For more information, please see the [Documentation](https://raiffeisen-dgtl.github.io/checkita-data-quality/).

## Project Build

Project is built using SBT. There are several JVM variables available to make builds more flexible:

* `-DSCALA_VERSION` - version of Scala used to build the project: either `2.12` or `2.13`.
* `-DPSARK_VERSION` - version of spark to build for. (support spark versions are: 3.2.0 and newer);
* `-DPKG_TYPE` - type of the package to build: `SNAPSHOT`, `DEV`, `RELEASE`.
  In addition, release candidate types are available: `RC1` - `RC5`.
* `-DPUBLISH_REALM` and `-DPUBLISH_URL` - type of the repository to publish package and its url.
  By default, package is published locally.
* `-DASSY_MODE` - mode used to assemble uber-jars. `WITHSPARK` and `NOSPARK` options are available.
  Accordingly, these options define whether to include spark dependencies in uber-jar or not.
  Often, when running application in cluster all the spark dependencies are already available and there
  is no need to include them in **Checkita** uber-jar.

Thus, depending on environment, it is possible to build:

* Uber-jar containing Checkita framework with all required dependencies including Spark ones.
  Such uber-jar can be used to run DQ jobs locally. Use `checkita-core / assembly` command to assemble uber-jar.
* When running DQ jobs in cluster it is required to put all required dependencies to spark library class path.
  Thus, it is possible to assemble uber-jar that will contain all required Checkita dependencies excluding 
  Spark and Scala libraries (use `-DASSY_MODE=NOSPARK` to exclude spark libraries from uber jar). Following sbt command
  need to be executed to assemble uber-jar with Checkita dependencies only: `checkita-core / assemblyPackageDependency`.
* As all framework dependencies are available via the spark library class path, it is only required to submit jar file
  with Checkita itself. One can be packaged using sbt command `checkita-core / package`.

## Contribution

Thank you for considering contributing to our project! We welcome contributions from everyone. By participating in 
this project, you agree to abide by our [Code of Conduct](docs/contribution/code-of-conduct.md).

Please take a moment to review our [Contribution guide](docs/contribution/contribution.md) in order to make the
contribution process as smooth as possible.

## License

Checkita Data Quality is [GNU LGPL](LICENSE.txt) licensed.

This project is a reimagination of [Data Quality Framework](https://github.com/agile-lab-dev/DataQuality) developed by Agile Lab, Italy.