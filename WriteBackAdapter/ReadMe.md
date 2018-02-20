## Write Back sample example

SDI Adapter SDK allows support of DML operations like insert and update as well.

To use this feature, you will need to first enable capablities realted to these.

```java
    @Override
	public Capabilities<AdapterCapability> getCapabilities(String version)
			throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		capbility.setCapability(AdapterCapability.CAP_SELECT);
		capbility.setCapability(AdapterCapability.CAP_INSERT);
		return capbility;
	}
```

* CAP_INSERT - The first capability we need to enable to allow writeback.

The writeback is triggered when user runs a insert into virtual table statement.
The statement will invoke the following APIs

* open()
* executePreparedInsert()
* putNext()  -> This API may be called multiple times until the entire data is pushed to the source.
* closeResultSet() 



### SQL usage

You can use the following sql to test the writeback out.

```sql
drop agent "DebugAgent" cascade;
create agent "DebugAgent" protocol 'TCP' host '<your host ip>' port 5050;
--Check if Agent is registered. Make sure host:port are properly setup.
select * from agents;

--Custom Adapter Registration
drop adapter "WriteBackAdapter" cascade;
create adapter "WriteBackAdapter" at location agent "DebugAgent";

--Run the following is remote source config or capabilities are updated.
alter adapter "WriteBackAdapter" refresh at location agent "DebugAgent";


select * from adapters;

select * from remote_sources;

--Create WriteBackAdapter Remote Source
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

--Check if above configuration is correct.
call CHECK_REMOTE_SOURCE('MyRemoteSource');

--Browse via ui.
 CALL "PUBLIC"."GET_REMOTE_SOURCE_OBJECT_TREE" ('MyRemoteSource','',?,?);

--Import the table to hana
drop table "SYSTEM"."test_table";
CREATE VIRTUAL TABLE "SYSTEM"."test_table" at "MyRemoteSource"."<NULL>"."<NULL>"."InMemoryTable";

--Select from source table.
select * From "SYSTEM"."test_table";

--Now let's insert into our test_table as it can act as target as well.
insert into "SYSTEM"."test_table" values(3,'from hana studio sql console');

--Let's ensure the table is updated.
select * From "SYSTEM"."test_table";



```