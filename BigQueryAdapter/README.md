# BigQuery Adapter for Hana SDI

## Overview

BQAdapter connects Hana to BigQuery and supports SELECT, INSERT, DELETE, UPDATE operations on BigQuery

For a complete list of capabilities, refer to BQAdapter.getCapabilities() method in BQAdapter.java


## Prerequisites
BIGQuery JDBC Driver - https://cloud.google.com/bigquery/providers/simba-drivers/
The BQAdapter was tested with the JDBC 4.1 compatible driver

Log4j - https://logging.apache.org/log4j/2.0/download.html

The Eclipse manifest file should have the following Plugins under the Dependencies section..
Require-Bundle: org.apache.log4j;bundle-version="1.2.15",
 com.sap.hana.dp.adapterframework;bundle-version="2.2.4"

Higher versions might work as well but needs to be tested.


## Configuration

BQAdapter uses a Properties file to specify GCP project information
bqadapter.properties file contains the following information::

Project ID - Google BigQuery Project ID
Email - OAuth Email Address (Service Account)
KeyFilePath - OAuth Secret Key file path
Driver - JDBC JAR Location of simba jar file

bqadapter.properties file should be present in /usr/sap/dataprovagent/adapters directory. 
See BQAdapter.open() method to see how it is used

The logging is controlled by the parameter 
framework.log.level=INFO  (other values include TRACE, DEBUG, WARNING, ERROR, FATAL) in /usr/sap/dataprovagent/dpagentconfig.ini


## Authors
Ravi Condamoor - Initial work - SAP Labs

## License
This project is licensed under the MIT License - see the LICENSE.md file for details



