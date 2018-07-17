package sap.ciep.sdi.webserviceadapter;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;


public abstract class WebserviceResponseHandler {
	protected static Logger logger = LogManager.getLogger("WebserviceResponseHandler");

	public static final int MAX_INLINE_LOB_LENGTH=Math.min(AdapterRow.MAX_ASCII_INLINE_LOB_LENGTH,AdapterRow.MAX_CLOB_INLINE_LOB_LENGTH);
	//public static final int MAX_INLINE_LOB_LENGTH=500;
	
	public abstract LinkedList<WebserviceResponseRecord> handleResponse( CloseableHttpResponse r, String url) throws AdapterException, ParseException, IOException;
	
	public int getLob(long lobId, byte[] bytes, int bufferSize)
			throws AdapterException {

		return 0;
	} 
}
