# Usage of Environment Variables and Extra Variables

Hocon configuration format supports variable substitution. This mechanism allows more flexible management of
both application configuration and job configuration.

Thus, configurations files are feed with extra variables that are read from system and JVM environment and can also be
explicitly defined at application startup.

For more information on how to explicitly define extra variables on startup,
see [Submitting Data Quality Application](../01-application-setup/02-ApplicationSubmit.md) chapter of the documentation.

In order to use system or JVM environment variables their names must match following regex expression:
`^(?i)(DQ)[a-z0-9_-]+$`, e.g. `DQ_STORAGE_PASSOWRD` or `dqMattermostToken`.
All environment variables that match this regex expression will be retrieved and available for substitution in both
application and job configuration files.

Typical use case for variable substitution is to provide secrets for connection to external systems. It is not a good
idea to store such information in configuration files and, therefore, there must be a mechanism to provide it at runtime.

> **IMPORTANT**: Variables are added to configuration files at runtime and are not stored in any form.