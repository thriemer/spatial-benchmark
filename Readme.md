# Evaluation of spatial databases for efficient data retrieval and functional suitability for location-based data products

This repository contains the source code to benchmark spatial database using a custom benchmark and evaluate the results using the Analytic Hierarch Process.

## Requirements

This benchmark was designed to run on Linux and uses the following command line tools:

- `docker`
- `pidstat`
- `pgrep`
- `du`

Some of these commands require root privileges to gather all information. To avoid running the benchmark with sudo, add the following line to `/etc/sudoers`. `$USERNAME` is the name of the user running the benchmark. The user must have root privileges

`$USERNAME ALL =(ALL) NOPASSWD: /usr/bin/pidstat,/usr/bin/du`

This benchmark uses the GraalVM native-image to avoid JVM warmups. Therefore, GraalVM CE 21.0.2 is required to compile this benchmark.

The benchmark needs spatial data which can be downloaded from https://download.geofabrik.de/europe.html
For running the benchmark the Turkey dataset is required. For running the tests the Lichtenstein dataset is required. The both need to be placed in the root folder of the project.

## Compiling the benchmark

The benchmark can be compiled using `./mvnw -Pnative native:compile`
The skip tests by adding the parameter: `-Dmaven.test.skip=true`

## Running the benchmark

To run the native image successfully the testcontainers checks have to be disabled using: `export TESTCONTAINERS_CHECKS_DISABLE=true`
The benchmark can be run by executing `./target/spatial`
There are two options `--skip-benchmark` which skips the benchmark and only runs the evaluation and `--clean-faulty-runs` which deletes all entries from the result table which have NaN values.

## Running the evaluation

The evaluation can be run using `./mvnw spring-boot:run -Dspring-boot.run.arguments=--skip-benchmark`