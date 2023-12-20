## [1.1.1](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.1.0...v1.1.1) (2023-12-20)


### Bug Fixes

* enhance kakfa windowing for streams and enable avro kafka message format ([#20](https://github.com/Raiffeisen-DGTL/checkita-data-quality/issues/20)) ([a5e9827](https://github.com/Raiffeisen-DGTL/checkita-data-quality/commit/a5e9827213da36ebc8932195018943204c3db309))

# [1.1.0](https://github.com/Raiffeisen-DGTL/checkita-data-quality/compare/v1.0.1...v1.1.0) (2023-12-08)


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
