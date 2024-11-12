# Check Failure Tolerance

Checkita is designed in such way that metric and check calculators never fails.
All errors that occur during either metric computation or evaluation of checks are captured and
returned to the user via metric errors and check status messages respectively. The unified status model
also serves to distinguish between failure of logical condition configured within metric or check and
runtime exception that was captured during their computation. 
See [Status Model used in Results](03-StatusModel.md) for more details.

From other perspective, sometimes it is quite handy to make DQ application fail when some of the checks fail.
For example, if DQ job is a part of an ETL process then it is good to have failure indication when DQ checks
are not satisfied. In order to support such behaviour Checkita offers different levels of check failure tolerance:

* `NONE` - DQ application will always return zero exit code, even if some of the checks have failed.
* `CRITICAL` - DQ application will return non-zero exit code only when some of the `critical` checks have failed.
  Checks configuration supports `isCritical` boolean flag which is used to identify critical checks. 
  By default, all checks are non-critical. 
  See [Checks Configurations](../03-job-configuration/09-Checks.md) for more details.
* `ALL` - DQ application will return non-zero exit code if any of the configured checks have failed.

The check failure tolerance is set in application configuration file as described 
in [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) section.