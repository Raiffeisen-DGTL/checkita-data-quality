## [2.0.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.7.2...v2.0.0) (2024-08-07)


### Bug Fixes

* added spark-xml lib for reading xml kafka topics ([#48](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/48)) ([22c3761](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/22c376187748974e1dae97bee9d32c32053f9053))


### Features

* add support of `*` for selection of all source columns in metric configuration ([#49](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/49)) ([ca09fe4](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/ca09fe4259039605cdfa52867b008807e21cc398))
* change project domain ([#52](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/52)) ([6020463](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/6020463420b120d1c0af97adb7766922f5e190c7))
* Checkita 2.0 release ([#45](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/45)) ([e747659](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/e7476598ada2b681ee6be478e5dce0e913799b0f))


### BREAKING CHANGES

* move project from ru.raiffeisen domain to org.checkita
domain.

Other changes include:

- fix backticks issue in GroupingDFMetricCalculator
- refactor DFMerticCalculator API to implicitly pass column types.
* major updates to Checkita Core that enables new
functionality and enhances existing one.

* new metrics engine based on Spark DF-API: improves stability and
performance of regular metrics computation. Supported in batch-jobs
only.
* checkpointing for streaming application: restart your application from
the same point where it stopped (or crushed).
* Checkita API Server - experimental MVP service that provides basic
functionality to work with configurations and DQ Storage.
* regular metrics refactoring to comply with SQL standards.
* new type of metrics: TREND metrics. Enables computing various
statistics over historical metric results.
* new type of checks: EXPRESSION. Allows to define check pass condition
using arbitrary boolean expression.
* enhanced formulas (for both composed metrics and expression checks):
formulas now supports basic mathematical functions.
* new Swagger documentation covering both Checkita Configurations and
API methods (still in development, will be completed shortly)
* support of Confluent Schema Registry to read schemas from.
* minor bug fixes.

## [1.7.2](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.7.1...v1.7.2) (2024-06-21)


### Bug Fixes

* filter NaN values from historical metric results in trend checks to avoid errors during computation of average metric value. ([#44](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/44)) ([d6da0f1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/d6da0f1c0d1259602def9ea39639186bc12fd31a))

## [1.7.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.7.0...v1.7.1) (2024-05-27)


### Bug Fixes

* prevent division by zero in check calculators ([#42](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/42)) ([c19578f](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/c19578f47a54c7de1483c51fdf87efe6843c0bba))

## [1.7.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.6.0...v1.7.0) (2024-05-13)


### Bug Fixes

* modify unique key constraint for regular metrics results table ([#38](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/38)) ([6a67b2f](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/6a67b2f34616caf2f377cfe05efda4e482695e40))
* update covariance metric logic ([#39](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/39)) ([0951ddc](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/0951ddcdf315e3408aa085989edf854216f385f4))


### Features

* adding schema registry reader and corresponding schema configuration ([#40](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/40)) ([be48204](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/be4820473515abec31f9af16b46d4300c6fba885))

## [1.6.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.5.0...v1.6.0) (2024-04-16)


### Features

* refactor kafka source API and enhance virtual sources resolving logic ([#37](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/37)) ([8ea9c23](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/8ea9c239b0c60c985a7ddc5eda8a3af4ab8c4171))

## [1.5.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.4.2...v1.5.0) (2024-03-29)


### Bug Fixes

* implement custom Spark collection accumulator to limit total number of collected errors ([#35](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/35)) ([f36e09f](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/f36e09ffa59ac6d51f8235be26f1a85c0f5f4ddf))
* update JobConfig and added test for config encryptor ([#34](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/34)) ([bc8bfd8](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/bc8bfd8f52d20d0c78adf449afdcfac550febf32))


### Features

* added functionality to reverse error collection logic for regular metrics with conditional criteria ([#36](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/36)) ([9244377](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/92443771af22d0a835a6cd1f04a5ea99a40ae200))

## [1.4.2](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.4.1...v1.4.2) (2024-03-20)


### Bug Fixes

* fixed bugs in HLL metric calculators and in email validation ([#33](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/33)) ([8009ed5](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/8009ed56d0a2b61acc96689ea6e31589bae39e37))

## [1.4.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.4.0...v1.4.1) (2024-03-14)


### Bug Fixes

* fixed bug in results saving logic ([#32](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/32)) ([613b596](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/613b596c2867ecdf71d565d759d39a17abf866dd))

## [1.4.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.3.2...v1.4.0) (2024-03-13)


### Features

* new db table to store metric errors ([#31](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/31)) ([37db1ed](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/37db1edc940d870815dda2ba86d3d2b277ba0650))

## [1.3.2](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.3.1...v1.3.2) (2024-03-11)


### Bug Fixes

* resolve config values substitution logic during encryption/decryption. ([#30](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/30)) ([b252f9d](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/b252f9d2ca4b9eec3c0ad92692b5fa323b4ed953))

## [1.3.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.3.0...v1.3.1) (2024-3-4)


### Bug Fixes

* update prepare-release scripts in order to fix auto-versioning and changelog formatting. ([#29](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/29)) ([9a30d59](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/9a30d59ec0b6e84225e0bbbf458546b50887c083))

## [1.3.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.2.1...v1.3.0) (2024-3-4)


### Features

* job configuration enhancements: added description and metadata fields and refactored hive source partition filtering ([#28](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/28)) ([7cf24db](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/7cf24db0d4999e064612f0658ddf0f836488ec86))

## [1.2.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.2.0...v1.2.1) (2024-2-26)


### Bug Fixes

* enhance casting methods for proper conversion of primitive values to target types ([#27](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/27)) ([744384b](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/744384b77cee7bac7c23d3af700ae7fb78c0a846))

## [1.2.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.1.1...v1.2.0) (2024-2-16)


### Bug Fixes

* docs update (describe new type of conn) and minor fixes ([#23](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/23)) ([c6abb81](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/c6abb811159df60499516594dde4900fd8a4ca1a))
* minor code fixes ([#25](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/25)) ([688929d](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/688929d3e8e41ff14d51d619d1acb3ba1990faa3))


### Features

* added ClickHouse connection ([#22](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/22)) ([d8add79](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/d8add795b47f126bbd707a6d9d6011c16dfa4caf))
* added Greenplum connection  ([#21](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/21)) ([5756eff](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/5756eff0d7803599129cb73e176bc6f8861a2df1))
* new entity in storage db - job config state ([#24](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/24)) ([fbe650c](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/fbe650c6cf79dc0e8991c8e88111ec482916b8ee))

## [1.1.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.1.0...v1.1.1) (2023-12-20)


### Bug Fixes

* enhance kakfa windowing for streams and enable avro kafka message format ([#20](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/20)) ([a5e9827](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/a5e9827213da36ebc8932195018943204c3db309))

## [1.1.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.0.1...v1.1.0) (2023-12-08)


### Features

* added new type connections ([#17](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/17)) ([205cbc8](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/205cbc8c989a2ce4a40de03c04a944651aec023f))
* adding functionality to run quality checks over streaming sources ([#19](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/19)) ([0895fed](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/0895fed14972a9311a53454146765ace522936b0))
* adding streaming sources and stream readers ([#16](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/16)) ([b5398fa](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/b5398fa2c8b22fac191e5853aed6f29d5d4f3821))

## [1.0.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.0.0...v1.0.1) (2023-11-13)


### Bug Fixes

* sbt assembly fixes and custom source restoring ([#12](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/12)) ([b6855ea](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/b6855ea89adcfb5af0a0b742ce4a0d8a42f343ac))

## 1.0.0 (2023-11-01)


### Bug Fixes

* fixes related to project build for different version of Spark ([#10](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/10)) ([c7218bd](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/c7218bd96b377a5e3cdfbcde4ee899bff5488a03)), closes [#7](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/7)


### Features

* added template support for email subject ([#9](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/9)) ([9246f7f](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/9246f7ff2511646088cf206a72667f4882c698ff)), closes [#6](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/6)
* adding duplicateValues metric ([#8](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/8)) ([da77daa](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/da77daab62c3dd9bb7e5d5fb4c340fa53a233c07)), closes [#5](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/5)
* Incorporating latest updates ([1fee747](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/1fee747152cf5bfd97531274843daff2faa81a38))
* Publishing major update of Checkita DQ ([09af3a2](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/09af3a2f065c0649849a026db41c5d3703d6233d)), closes [#3](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/3)

## 0.3.5 (2023-09-19)

### Bug Fixes

* Ensure that JDBC connection is alive prior saving results.
  There could be situation when Application runs quite long and DB server can close idle connection due to
  inactivity. In such cases it is required to open it again before saving results.

## 0.3.4 (2023-09-14)

### Features

* Added support of customisable email subject templates.

### Bug Fixes

* Fixed duplicateValues metric: some duplicate values can only be determined during metric calculator merge.
* Fix sender name for check Alert. It was hardcoded but need to refer the name configured in application configuration file.
* Fixed email encoding: changed to UTF-8

## 0.3.3 (2023-09-04)

### Features

* Added new metric: duplicateValues.
* Enhanced joinSql virtual source by allowing to supply it with arbitrary number of parent sources.
* Enhanced table source by allowing to supply it with query to execute (on the DB side) and thus, read only query results.
* Docs update

## 0.3.2 (2023-08-25)

### Features

* Added email sender name customization
* Added functionality to provide html and markdown templates to build body of check alerts and 
  summary report messages when sending them to either email or Mattermost.
* Added custom source
* Docs update

### Bug Fixes

* Pass both source and virtual sources to target processors
* Fix json serialization bugs.

## 0.3.1 (2023-08-17)

### Bug Fixes

* Fix params json when sending results to kafka
* Add sourceId and sourceKeyFields to errorCollection reports (or kafka messages)
* Fix CSV headers for checkAlert attachments
* Fix column metric result representation when saving to DB (sourceId and metric description were mixed)
* Add MD5 hash to message key when sending results to Kafka in order to ensure idempotent message consumption.

## 0.3.0 (2023-07-31)

### Features

* Change DB model:
    * Added referenceDateTime and executionDateTime
    * Type in DB - timestamp with tz
    * Render format can be setup in application.conf
    * Job-conf variables are changed to referenceDateTime and executionDateTime
    * Changed init sql script and also added alter sql script
* Added option to sent aggregated messaged to Kafka: one per each target type
* Added option to run DQ in Shared Spark Context
* Added new types of history DB: Hive and File (both managed by spark without extra services)

### Bug Fixes

* Fixed SQL checks
* Made DQ case-insensitive in terms of column names
* Docs updates

## 0.2.0 (2023-06-21)

### Features

* Adding support of Spark 2.4+ and Spark 3+
* Project is rebuild for Scala 2.12.18
* Added test for HdfsReader

### Bug Fixes

* Fixed HdfsReader in terms of loading fixed-width files: added casting to requested column types
* HBase source is temporarily turned off (due to connector is incompatible with newer versions of Scala and Spark)

## 0.1.10 (2023-06-15)

### Features

* Adding Kafka support:
    * New section in run configuration to describe connection to Kafka Brokers
    * New type of source to read from Kafka topic
    * Output of all targets to Kafka topic
* Adding Mattermost notifications:
    * New section added to application configuration to describe connection to Mattermost API.
    * CheckAlerts and summary reports can be sent to Mattermost.
    * Notifications can be sent to both channels and user direct messages.
* Added new DQ application argument -v to pass extra variables to be prepended to application configuration file. Can be used to pass secrets for email, mattermost and storage DB on startup.
* Documentation is updated according to new features.

### Bug Fixes

* Fixed email configuration for cases when smpt support anonymous connection (user and password are undefined)

## 0.1.9 (2023-05-17)

### Features

* Enable mailing notifications
* Added summary section to targets to set up summary reports sending via email
* Added checkAlerts section to targets to set up critical check alerts via email
* Added errorCollection section to targets to set up error collection (stored to HDFS only for now)
* Refactored metric error collection: single file with unified format will be written
* Modified config model for virtualSources to allow set the saving options directly when declaring virtual source.
* Update application config files with default production settings for mailing
* Update documentation

### Bug Fixes

* Update spark accumulator for metric error collection
* Add TLS Support to mailer
* Change mail attachments to ByteArrayDataSource in order to provide attachment as a byte stream instead of file.
* Minor documentation fixes
* Prevent empty file creation when errors accumulator is empty (this is also the case when keyFields are not set)

## 0.1.8 (2023-04-20)

### Features

* Enable metric error collection.
* Add keyFields to source for purpose of metric error collection.
* Update metrics to make them Statusable whenever possible.
* Enhance DQ command line arguments to allow passing arbitrary number of variables on startup which will be added to
  DQ configuration file in runtime and may be references within it.
* Updated documentation to reflect aforementioned changes.

### Bug Fixes

* Updated metric tests to cover Statusable calculation.
* Refactored configuration file parsing: now the variables are prepended to configuration file by means of streams
  and no temporary file is created.

## 0.1.7 (2023-04-04)

### Features

* Added numberNutBetween metric
* Added new results view to database init sql
* Added jobId to metric results model

### Bug Fixes

* Fixed composed metric calculator: increased power operation priority
* Fixed database config reading
* Fixed RDBMS source reading
* Fixed HistoryDBManager to additionally filter query results by jobId

## 0.1.6 (2022-08-23)

### Features

* Added unit tests for column and file metric calculators;
* Refactored date-related metrics to make them work with timestamp column type correctly;

### Bug Fixes

* Fix build.sbt for production environment
* Add hive meta schema initialisation (script for schema initialisation in DQ database, not part of DQ application);
* Fix schema creation in db-init.sql (script for schema initialisation in DQ database, not part of DQ application);
* Other minor bug fixes. 

## 0.1.5 (2022-07-01)

### Features

* Added documentation
* SBT build is updated in terms of assembling uber-jars.

## 0.1.4 (2022-06-22)

### Features

* New ConfigReader is added
* Format of metrics configuration file should be set within application.conf (0.x for old format 1.x for new format)
* Documentation on how to fill various sections of job configuration file is added.
