# BigQuery Adapter for Hana SDI

## Overview

BQAdapter connects Hana to BigQuery and supports SELECT, INSERT, DELETE, UPDATE operations on BigQuery


## Capabilities
BQAdapter has a robust set of set of Capabilities, including JOIN relocation and Push-down query

```java
@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capability = new Capabilities<AdapterCapability>();
		//enabled capabilities
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_AND);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_IN);
		capability.setCapability(AdapterCapability.CAP_OR);
		capability.setCapability(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_PROJECT);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_DIST_AGGREGATES);
		capability.setCapability(AdapterCapability.CAP_BETWEEN);
		capability.setCapability(AdapterCapability.CAP_DISTINCT);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
		capability.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capability.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		capability.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capability.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_LIKE);
		capability.setCapability(AdapterCapability.CAP_HAVING);
		capability.setCapability(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capability.setCapability(AdapterCapability.CAP_AGGREGATES);
		capability.setCapability(AdapterCapability.CAP_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_GROUPBY);
		capability.setCapability(AdapterCapability.CAP_JOINS);
		capability.setCapability(AdapterCapability.CAP_JOINS_FULL_OUTER);
		capability.setCapability(AdapterCapability.CAP_JOINS_OUTER);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_DELETE);
		capability.setCapability(AdapterCapability.CAP_UPDATE);
		capability.setCapability(AdapterCapability.CAP_INSERT);
		capability.setCapability(AdapterCapability.CAP_INSERT_SELECT);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);

  return capability;
}
```

Refer BQAdapter.getCapabilities() method in BQAdapter.java


## Prerequisites
BIGQuery JDBC Driver - https://cloud.google.com/bigquery/providers/simba-drivers/
The BQAdapter was tested with the JDBC 4.1 compatible driver

Log4j - https://logging.apache.org/log4j/2.0/download.html

The Eclipse manifest file should have the following Plugins under the Dependencies section..
Require-Bundle: org.apache.log4j;bundle-version="1.2.15",
 com.sap.hana.dp.adapterframework;bundle-version="2.2.4"

Higher versions might work as well but needs to be tested.


## Configuration

BQAdapter uses a Properties file to specify GCP project information.
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
This project is licensed under the  Apache 2.0 License - see the LICENSE.md file for details



