package sap.ciep.sdi.webserviceadapter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;

public abstract class WebserviceResponseRecord {

	protected static Logger logger = LogManager.getLogger("WebserviceResponseRecord");
	
	/**
	 * Append this record to response to HANA.
	 * @param rows
	 * @throws AdapterException 
	 */
	public abstract void appendTo(AdapterRowSet rows) throws AdapterException;
}
