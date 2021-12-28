# Reshape on Apache Flink
This is the code for the implementation of Reshape on Apache Flink 1.13. More details about Flink and how to build it can be found at [Flink's github](https://github.com/apache/flink).

## Building the project
```console
git clone https://github.com/Reshape-skew-handling/reshape-on-flink.git
cd reshape-on-flink
mvn clean package -Drat.numUnapprovedLicenses=1000 -DskipTests -Dfast -Dcheckstyle.skip
```
## Running the project:

This step is the same as Flink's original instruction. However, since Reshape is already a built-in functionality of Flink in this repo, you can pass `-DenableReShape=true` to flink-conf.yaml to enable reshape when starting Flink.

## Resahpe parameters:

Some parameters of reshape can be tuned:
| Parameter name  | Type | Default value | Usage |
| ------------- | ------------- |  ------------- |  ------------- |  
| enableReShape  | Boolean      |  false  | enable/disable reshape | 
| hiThreshold  | Double  |  0.8  | worker with busy ratio higher than this threshold will be treated as skewed | 
| loThreshold  | Double  |  0.2  | worker with busy ratio lower than this threshold will be treated as free | 
| reshapeFreq  | Long      |  10000  | interval of triggering one reshape iteration (unit:ms) | 
