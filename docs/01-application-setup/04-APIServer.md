# API Server

**Experimental. Subjected to change.***

In addition to main Checkita Framework, that performs Data Quality jobs, it is possible
to set up and run separate API service which brings additional functionality in interacting
with Checkita configuration and results.

> **IMPORTANT**. API server is in its early stage of developments and its
> functionality will be improved as it matures.

Thus, at current moment API server supports following functionality:

* Configuration:
    * Validation of application configuration.
    * Validation of job configuration.
* DQ Storage:
    * Fetch overall summary for all jobs in DQ storage.
    * Fetch actual job state that was run.
    * Fetch job results for given datetime interval.

See [Swagger Doc](../swagger/index.md#swagger-doc) for more details on Checkita API Server methods.
