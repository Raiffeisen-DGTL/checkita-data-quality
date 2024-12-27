# Checkita Data Quality

**Latest Version: 2.2.0**

**Checkita** is a data quality framework written is Scala 2 which uses Spark 3.2+ as a computation core.
This framework is used to perform parallel and distributed quality checks on big data environments.
Its functionality allows calculating various metrics over the large datasets and perform multiple checks to ensure quality of these datasets.

For more information, please see the [Documentation](https://docs.checkita.org/).

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

## Releases to Maven Central Repository

Starting from Checkita 2.0 the project releases are published to Maven Central repository. As it was already stated,
Checkita supports multiple Spark versions starting from 3.2.0 and up to 3.5.1 (currently). The code base of Checkita
does not change between supported version of Spark. However, the Spark transitive dependencies DO change for
various version. In order to keep reasonable number of packages being released to Maven Central repository 
per each Checkita version, we only publish Checkita packages tied to minor Spark version like 3.2.0 or 3.3.0 
and skip patch Spark versions. Thus, Checkita packages have following versioning scheme: 
`<checkita-version>-<spark-minor-version>`. For example: `checkita-core_2.12-2.0.0-3.2` is build for Scala 2.12
and Spark 3.2.0.

We also provide FULL lists of dependencies for each of supported Spark versions in raw text format.
You can fund these lists by links in the table below:

| Spark \ Scala | 2.12                                                               | 2.13                                                               |
|---------------|--------------------------------------------------------------------|--------------------------------------------------------------------|
| 3.2.0         | [view](docs/dependencies/checita-core-dependencies-2.12-3.2.0.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.2.0.txt) |
| 3.2.1         | [view](docs/dependencies/checita-core-dependencies-2.12-3.2.1.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.2.1.txt) |
| 3.2.2         | [view](docs/dependencies/checita-core-dependencies-2.12-3.2.2.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.2.2.txt) |
| 3.2.3         | [view](docs/dependencies/checita-core-dependencies-2.12-3.2.3.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.2.3.txt) |
| 3.2.4         | [view](docs/dependencies/checita-core-dependencies-2.12-3.2.4.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.2.4.txt) |
| 3.3.0         | [view](docs/dependencies/checita-core-dependencies-2.12-3.3.0.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.3.0.txt) |
| 3.3.1         | [view](docs/dependencies/checita-core-dependencies-2.12-3.3.1.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.3.1.txt) |
| 3.3.2         | [view](docs/dependencies/checita-core-dependencies-2.12-3.3.2.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.3.2.txt) |
| 3.3.3         | [view](docs/dependencies/checita-core-dependencies-2.12-3.3.3.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.3.3.txt) |
| 3.3.4         | [view](docs/dependencies/checita-core-dependencies-2.12-3.3.4.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.3.4.txt) |
| 3.4.0         | [view](docs/dependencies/checita-core-dependencies-2.12-3.4.0.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.4.0.txt) |
| 3.4.1         | [view](docs/dependencies/checita-core-dependencies-2.12-3.4.1.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.4.1.txt) |
| 3.4.2         | [view](docs/dependencies/checita-core-dependencies-2.12-3.4.2.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.4.2.txt) |
| 3.4.3         | [view](docs/dependencies/checita-core-dependencies-2.12-3.4.3.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.4.3.txt) |
| 3.5.0         | [view](docs/dependencies/checita-core-dependencies-2.12-3.5.0.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.5.0.txt) |
| 3.5.1         | [view](docs/dependencies/checita-core-dependencies-2.12-3.5.1.txt) | [view](docs/dependencies/checita-core-dependencies-2.13-3.5.1.txt) |

## Contribution

Thank you for considering contributing to our project! We welcome contributions from everyone. By participating in 
this project, you agree to abide by our [Code of Conduct](docs/contribution/code-of-conduct.md).

Please take a moment to review our [Contribution guide](docs/contribution/contribution.md) in order to make the
contribution process as smooth as possible.

## License

Checkita Data Quality is [GNU LGPL](LICENSE.txt) licensed.

This project is a reimagination of [Data Quality Framework](https://github.com/agile-lab-dev/DataQuality) developed by Agile Lab, Italy.