# Working with Date and Time

There are two type of datetime instances used in order to identify various Data Quality job runs. These are:

* `referenceDate` - identifies date for which the job is run. This datetime usually indicates for which
  period data is read and checked.
* `executionDate` - stores actual application start datetime and used to indicate when exactly data quality job is run.

Typical case is when we run some ETL pipeline after "closure of business", e.g. at midnight.
Thus, the `referenceDate` will refer to a previous day, while `executionDate` will have value of actual start of
data quality job. It is likely that we would like to represent these values differently. Thus,
in application configuration we can configure different formats for `referenceDate` and `executionDate`

As `referenceDate` can point to a date in the past, then it is allowed to explicitly provide its values
on application startup. If value of `referenceDate` is not provided, then it is set to datetime of actual start of
data quality job. See [Submitting Data Quality Application](../01-application-setup/02-ApplicationSubmit.md) 
chapter for more information on application startup arguments.

Both of these datetime instances are widely used across framework. Thus, whenever string representation of them
is required, it is obtained using datetime parameters set in the application configuration file.

It also should be noted, that datetime rendering is performed with respect to timezone in which the application
is running. Timezone is also set in application configuration file. The `UTC` time zone is used by default.

The last but not least: we avoid using datetime string representation when storing results into storage database.
Both `referenceDate` and `executionDate` are converted to timestamp at `UTC` timezone, instead.
This ensures stable results querying from storage independent on datetime configuration parameters.
See [Data Quality Results Storage](../01-application-setup/03-ResultsStorage.md) chapter for more information 
on results storage.

> **IMPORTANT**: Actual string representation of `referenceDate` and `exectionDate` are always added to configuration
> files as extra variables. For more details on extra variables usage in configuration files, see 
> [Usage of Environment Variables and Extra Variables](02-EnvironmentAndExtraVariables.md) chapter.