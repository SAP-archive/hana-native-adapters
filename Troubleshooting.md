## Troubleshooting

In case of issues during adpater development, if you run into issues please grab the following 

* Framework.trc files: You can find this either in eclipse/log folder or /usr/sap/dataprovagent/log folder, depending on where you started your agent
                       You can add -Dframework.log.location to customize the log directory in cause you could not find where the log file is.
                       
* Server traces: You should grab dpserver.trc and indexserver.trc files

* HANA views: To get a full picture of what is registered in the system, please collect the following views.

```sql
-- Get all agents registered in hana
select * from PUBLIC.AGENTS;

--View hana services, what port they are running on and so on.
select * From m_services; 

-- get all registered adapter
select * from PUBLIC.ADAPTERS;

-- get all remote sources created
select * from remote_sources; 

-- get hana overview like versions, memory, etc.
select * from PUBLIC.M_SYSTEM_OVERVIEW;

-- get agent overview like version, memory, etc
select * from PUBLIC.M_AGENTS; 

--check for initial loads
select REMOTE_SOURCE_NAME, REMOTE_STATEMENT_STRING from m_remote_statements order by START_TIME DESC;  

-- get adapter runtime information for running replication. This allows us to see if adapter is behind on scanning or not.
SELECT * from M_Remote_source_Statistics; 

--Agent Connectivity issues
select * from "_SYS_STATISTICS"."STATISTICS_ALERTS" where alert_id = 700  and index = '<agent>' order by alert_timestamp desc; 

--get dictionary status
select * from m_remote_sources; 

--Real time replication related views. If you are developing adapters based on AdapterCDC interface
--The following views will give you a idea of what adapter is getting from the remote source, sending to HANA, what is HANA applying and so on.
select * from PUBLIC.m_remote_subscriptions;
select * from PUBLIC.remote_subscriptions; 
select * from M_REMOTE_SUBSCRIPTION_STATISTICS; -- get runtime information for all active replications.
select * from remote_subscription_exceptions; 
SELECT * from M_Remote_source_Statistics where sub_component = 'UI';
select subscription_name, end_marker_time, last_processed_transaction_time from m_remote_subscriptions ; 
```


### Enabling extra logging levels

For SDI Agent you can change the framework.log.level to DEBUG, TRACE or ALL, in the dpagentconfig.ini.

However in eclipse, you can change the run configuraiton and update the vm argument -Dframework.log.level=ALL to enable extra logging.

For HANA Server you can use the following to change log level based on the area you are developing for. For Federation or Virtualization vs Real time replication.
Once you are dong with debugging, it is recommended to switch the levels back to info to avoid overflow of log messages.

```sql
--By default, hana logging rollsover after 10 files but you can change the max files to keep using the following.
--Note: if you have limited disk space do not increase them.
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM')    SET ('trace', 'maxfiles')   = '50' WITH RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'SYSTEM') SET ('trace', 'maxfiles')   = '50' WITH RECONFIGURE;

--Basic debug levels for any area
 ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'SYSTEM') SET ('trace', 'dpapplier')  = 'debug' WITH  RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM')    SET ('trace', 'dpapplier')  = 'debug' WITH  RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM')    SET ('trace', 'dpserver')   = 'debug' WITH  RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM')    SET ('trace', 'dpframework')= 'debug' WITH  RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM')    SET ('trace', 'dpreceiver') = 'debug' WITH  RECONFIGURE;
 ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'SYSTEM') SET ('trace', 'DPAdapterManager') = 'debug' WITH  RECONFIGURE;

-- For Virtual Function and Virtual Procedures
 ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'SYSTEM') SET ('trace', 'dpVirtualFxn') = 'debug' WITH  RECONFIGURE;

-- For getting exact message send to/from agent. Enabling this will overflow logs and slows down the entire process, so do it with caution.
 ALTER SYSTEM ALTER CONFIGURATION ('dpserver.ini', 'SYSTEM') SET ('trace', 'dpframeworkmessagebody') = 'debug' WITH  RECONFIGURE;
 
 -- For debugging every SQL statement
 ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'SYSTEM') SET ('trace', 'FedTrace') = 'debug' WITH  RECONFIGURE;
```
