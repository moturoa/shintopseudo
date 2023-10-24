![](https://badgen.net/badge/shintolabs/production/green)
# shintopseudo: data pseudonimiseren


## What is this

R package used for anynomization, typically deployed in a cluster maintained by the tenant. Reads CSV files,
and based on a configuration file will do one or more of the following to any of the columns:
- anonymization : secure hashing of values, with value/hash pairs saved in an SQLite database
- symmetric encryption: using a secret, encrypt the data with libsodium
- remove: delete columns
- keep: keep only these columns

This package is normally used inside a docker container, which reads data on a tenant secure cluster, and outputs
data without sensitive information used in our data applications. See repos `pseudomaker` for the standard dockerized implementation, and `deployments-ede`, `hk-azure-deployment`, `ede_izm_configuratie`, `ede_datadienst_configuratie` for
configuration examples.


## Installation

```
devtools::install_github("moturoa/shintopseudo")
```


## Test

Open `test/config_brp_test.yml` for a test configuration file with comments.
Go to `test/test_package.R` and run the script.


## Contact

Remko Duursma


