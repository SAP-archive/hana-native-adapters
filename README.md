# Hana Native Adapter Development in Java

## Overview

While Hana can be seen as yet another database to read data from or load data in using external tools, Hana has built-in capabilities for batch and realtime data provisioning and data federation.
Any adapter registered in Hana can be used to query remote sources using regular Hana SQL commands and, if the adapter supports this capability, push changes into Hana tables or tasks (=transformations). As all adapters written using the Adapter SDK are no different from the SAP provided adapters, they can be used by all tools and editors as well, e.g. Replication Editor, Task Flow Editor, Enterprise Semantic Search,.....

The aim of this repository is to provide adapters as source code to access more sources. These adapters are provided by SAP employees, partners and customers on a as-is basis with the indention of being used in production environments and to act as examples.

For more details refer to [Hana Smart Data Integration](http://scn.sap.com/community/developer-center/hana/blog/2014/12/08/hana-sps09-smart-data-integration--adapters) at SCN.
The Adapter SDK is part of the Hana EIM option and as such fully documented in [help.sap.com](http://help.sap.com/saphelp_hana_options_eim/helpdata/en/d4/b02f02b92a4242a57a6d1a9b84ea7c/content.htm?current_toc=/en/8f/e39f672f4542ada5ff9d821b296efa/plain.htm&show_children=true)


## Contributing

This is an open source project under the Apache 2.0 license, and every contribution is welcome. Issues, pull-requests and other discussions are welcome and expected to take place here. 

