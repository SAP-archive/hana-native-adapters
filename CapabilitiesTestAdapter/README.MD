# The CapabilitiesTestAdapter

This adapter should help understanding the various adapter level capabilities. 
There is a more detailed explanation of Capabilities in general in [SAP SCN](http://scn.sap.com/community/developer-center/hana/blog/2015/09/16/hana-adapter-sdk--adapter-capabilities).

## Usage

The adapter provides two tables, SQLTABLE and ADAPTERCAPS. The SQLTABLE is a table with one row and two column, a first integer column and a second text column containing the received SQL text. So when a "select * from v_sqltable" is executed against a virtual table pointing to the adapter SQLTABLE, this statement is analyzed be the Hana SDA Federation logic and Hana SQL optimizer and whatever is sent to the adapter, the SQLTEXT column contains that plain text.
By querying the ADAPTERCAPS table the adapter capabilities (not table or column capabilities, these are not used by the adapter) are listed and the information if they are turned on or off. Using an update statement the settings can be changed.

Attention: The ADAPTERCAPS table shows what the adapter would return currently whenever the method getCapabilities() is called. But this method is not called unless the adapter is either created in Hana or refreshed by Hana. Also, the capabilities are kept in memory only, meaning if the adapter is restarted it has the default capabilities only - all changes are lost.

For this reason the following code segments should be used to play with the capabilities (assuming the remote source is called "captest", the agent name is "mylaptop" and a virtual table called v_adaptercaps was created):
* `update v_adaptercaps set isset = 'TRUE' where CAPABILITY = 'CAP_NONEQUAL_COMPARISON';`
* `alter adapter "CapabilitiesTestAdapter" refresh at location agent "mylaptop";`
* `drop table v_sqltable;`
* `create virtual table v_sqltable at "captest"."<NULL>"."<NULL>"."SQLTABLE";`
* `select * from v_sqltable where rownumber>=1;`



