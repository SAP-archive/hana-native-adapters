## Virtual Function and Virtual Procedure demo adapter

SDI Adapter SDK allows virtual function and virtual procedures as well.

To ensure to get features, make sure your manifest file uses Required bundle instead of Import Package.

ex: Minimum required version is 2.0 or higher.

```
Require-Bundle: org.apache.log4j;bundle-version="1.2.15",
 com.sap.hana.dp.adapterframework;bundle-version="2.2.4"
```

### Virtual Function API Calls 

* open()
* validateCall()
* executeCall()  -> This API may be called to execute the function
* getNext() -> This will be invoked multiple times until the adapter sends the empty rowset back.

### Virtual Procedure API Calls

Please refer to com.sap.hana.dp.adapter.sdk.CallableProcedure.java for latest documentation.

In brief, Virtual Procedure allows adapter to expose remote procedures to HANA. Adapter will have to implement this class along with Adapter.prepareCall method and provide an object to framework.

Agent framework will execute the virtual procedure call as described below.

Importing of Virtual Procedure. 
	
	Step 1: Adapter will return an browse object with node type PROCEDURE in Adapter.browseMetadata() call
	
	Step 2: Adapter will return ProcedureMetadata object describing the remote procedure metadata in Adapter.importMetadata() call
	
	Step 3: Adapter will then validate the metadata again when server is ready to save the virtual procedure in its catalog in Adapter.validateCall() call. 
	

Execution of Virtual Procedure 
	
	Step 1: Adapter will need to create an object of this class and instantiates all necessary objects required to perform this procedure call in Adapter.prepareCall() call. Prepare call will have all scalar inputs available at that moment.
	
	Step 2 (optional): If there are any Table IN parameter, server will call putNextRowSet with the index of the parameter along with the data.
	
	Note: This call will be executed multiple times until Adapter gets an empty result set marking the end of data.
				
	Step 3 : Framework will call executeCall() on this instance.
	
	Step 4 (optional): If there is any scalar OUT parameter, framework will call getNextScalar() with index of the parameter and Scalar parameter object where adapter needs to fill the value with.
	
	Step 5 (optional): If there is any table OUT paramter, framework will call getNextRowSet() with index of the parameter and AdapterRowSet object where adapter needs to fill the value with. 
	
	Step 6 : Once the execution is done and the above optional may be called, at the end of the call, server will call Adapter.closeResultSet indicating it has finished processing the result sets. Adapter will need to make sure they close temporary result sets and lob buffers.
	

Note: Virtual Procedure are not read only, they may change state on remote source, which is source-dependent.




### SQL usage

You can use the following sql to test the virtual function and procedures out.

```sql

drop agent "DebugAgent" cascade;
create agent "DebugAgent" protocol 'TCP' host '<hostIp>' port 5050;
--Check if Agent is registered. Make sure host:port are properly setup.
select * from agents;

--Custom Adapter Registration
drop adapter "DemoAdapter" cascade;
create adapter "DemoAdapter" at location agent "DebugAgent";

--Run the following is remote source config or capabilities are updated.
alter adapter "DemoAdapter" refresh at location agent "DebugAgent";


select * from adapters;

select * from remote_sources;

--Create DemoAdapter Remote Source
drop remote source "MyRemoteSource" cascade;
CREATE REMOTE SOURCE "MyRemoteSource" ADAPTER "DemoAdapter" AT  LOCATION agent "DebugAgent" 
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

--Check if above configuration is correct.
call CHECK_REMOTE_SOURCE('MyRemoteSource');

--Browse via ui.
 CALL "PUBLIC"."GET_REMOTE_SOURCE_OBJECT_TREE" ('MyRemoteSource','',?,?);

--Import the table to hana
drop table "SYSTEM"."test_table";
CREATE VIRTUAL TABLE "SYSTEM"."test_table" at "MyRemoteSource"."<NULL>"."<NULL>"."DemoTable";

--Select from source table.
select * From "SYSTEM"."test_table";


--Using Virtual Functions. You should always use WebIDE for Virtual Functions and Procedures but for testing, you can use SQL.
--Use webide http://<hana-hostname>:8000/sap/hana/ide/catalog/
--First get the configuration json you can copy and paste the json into the CREATE statement below.drop function demo_function;
CALL "PUBLIC"."GET_REMOTE_SOURCE_FUNCTION_DEFINITION" ('MyRemoteSource','DemoFunction',?,?,?);

CREATE virtual FUNCTION demo_function(A INT, B INT) RETURNS TABLE (SUM INT, TEXT NVARCHAR(255)) CONFIGURATION'{
  "__DP_UNIQUE_NAME__": "DemoFunction",
  "__DP_USER_DEFINED_PROPERTIES__": {
    "test": "some attributes"
  },
  "__DP_VIRTUAL_PROCEDURE__": false
}' AT  "MyRemoteSource";

--You can now invoke you demo function via the following SQL.
select * from demo_function(2,2);

--Virtual Procedures.
drop procedure demo_procedure;
--First get the configuration json you can copy and paste the json into the CREATE statement below.
CALL "PUBLIC"."GET_REMOTE_SOURCE_FUNCTION_DEFINITION" ('MyRemoteSource','DemoProcedure',?,?,?);

CREATE VIRTUAL PROCEDURE demo_procedure(IN inputParameter int, OUT OUT_TABLE TABLE( userinput INT, NVARCHAR_COLUMN NVARCHAR(2000)) )
 CONFIGURATION '
{
  "__DP_UNIQUE_NAME__": "DemoProcedure",
  "__DP_VIRTUAL_PROCEDURE__": true
} 
' AT  "MyRemoteSource";

--You can now invoke you demo procedure via the following DDL.
call demo_procedure(4, ?);

```

## Note

This is bare minimal code to show how to get virtual function and procedure enabled with a in-memory table.

* Hana Studio does not support these features. Please use WebIDE which should be accessible at http://<hana-hostname>:80<hana-instance-id>/sap/hana/ide/catalog/
