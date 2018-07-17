# Hana Native Adapter Development in Java

## Overview

While Hana can be seen as yet another database to read data from or load data in using external tools, Hana has built-in capabilities for batch and realtime data provisioning and data federation.
Any adapter registered in Hana can be used to query remote sources using regular Hana SQL commands and, if the adapter supports this capability, push changes into Hana tables or tasks (=transformations). As all adapters written using the Adapter SDK are no different from the SAP provided adapters, they can be used by all tools and editors as well, e.g. Replication Editor, Task Flow Editor, Enterprise Semantic Search,.....

The aim of this repository is to provide adapters as source code to access more sources. These adapters are provided by SAP employees, partners and customers on a as-is basis with the indention of being used in production environments and to act as examples.

For more details refer to [Hana Smart Data Integration](http://scn.sap.com/community/developer-center/hana/blog/2014/12/08/hana-sps09-smart-data-integration--adapters) at SCN.
The Adapter SDK is part of the Hana EIM option and as such fully documented in [help.sap.com](http://help.sap.com/saphelp_hana_options_eim/helpdata/en/d4/b02f02b92a4242a57a6d1a9b84ea7c/content.htm?current_toc=/en/8f/e39f672f4542ada5ff9d821b296efa/plain.htm&show_children=true)


## Contributing

This is an open source project under the Apache 2.0 license, and every contribution is welcome. Issues, pull-requests and other discussions are welcome and expected to take place here. 

## Sample Adapters

 * WriteBackAdapter: Shows the writeback capabilities, meaning read data from hana and push to remote source
 * DemoAdapter: Shows the virtual function and virtual procedures capablities.
 * JDBCAdapter: Shows how to connect to the remote DB using jdbc and also rewriting HANA SQL Query to remote format
 * RealTimeTestAdapter: Shows Real time replication capablities.
 * WebServiceAdapter: Shows how to call httpserver and expose the response via Adapter SDK. See [link](https://blogs.sap.com/2018/06/29/provisioning-data-from-web-services-with-custom-adapter-using-sap-hana-sdi-sdk/)

## SQL to use custom adapter

```sql
--Note: You do not need to run drop command but this provides you an idea what command to run for clean up

drop agent "DebugAgent" cascade;

create agent "DebugAgent" protocol 'TCP' host '<your host ip>' port 5050;

--Check if Agent is registered. Make sure host:port are properly setup.
select * from agents;

--Custom Adapter Registration
drop adapter "WriteBackAdapter" cascade;
create adapter "WriteBackAdapter" at location agent "DebugAgent";

--Run the following if remote source config or capabilities are updated.
alter adapter "WriteBackAdapter" refresh at location agent "DebugAgent";


--Check for adpater registration.
select * from adapters;

--Create WriteBackAdapter Remote Source. Always use UI for doing the following but you can use SQL for testing.
drop remote source "MyRemoteSource" cascade;
CREATE REMOTE SOURCE "MyRemoteSource" ADAPTER "WriteBackAdapter" AT  LOCATION agent "DebugAgent" 
 CONFIGURATION '<?xml version="1.0" encoding="UTF-8"?>
 <ConnectionProperties name="testParam">
	<PropertyEntry name="name">Hana_Studio</PropertyEntry>
 </ConnectionProperties>
'	
 WITH CREDENTIAL TYPE 'PASSWORD' USING 
 '<CredentialEntry name="credential">
		<username>testuser</username>
		<password>testpassword</password>
</CredentialEntry>';

--HANA stores your remote source configuration in the following table.
select * from remote_sources;


--Check if above configuration is correct.
call CHECK_REMOTE_SOURCE('MyRemoteSource');

--Browse via ui.
 CALL "PUBLIC"."GET_REMOTE_SOURCE_OBJECT_TREE" ('MyRemoteSource','',?,?);

--Import the table to hana
drop table "SYSTEM"."test_table";
CREATE VIRTUAL TABLE "SYSTEM"."test_table" at "MyRemoteSource"."<NULL>"."<NULL>"."InMemoryTable";

--Select from source table.
select * From "SYSTEM"."test_table";

```
