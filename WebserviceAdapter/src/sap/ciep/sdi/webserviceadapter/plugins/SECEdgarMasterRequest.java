package sap.ciep.sdi.webserviceadapter.plugins;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Metadata;

import sap.ciep.sdi.webserviceadapter.WebserviceRequestHandler;

public class SECEdgarMasterRequest extends WebserviceRequestHandler{

	final static String URL="https://www.sec.gov/Archives/edgar/full-index/master.idx";  
	
	@Override
	public CloseableHttpResponse exec(Metadata func, HashMap<String, String> connProps)
			throws AdapterException, ClientProtocolException, IOException, URISyntaxException {
		
		CloseableHttpClient httpclient=WebserviceRequestHandler.getDefaultHttpClient();

		HttpGet httpget = new HttpGet(URL);
		CloseableHttpResponse resp = httpclient.execute(httpget);
		logger.debug("HttpAuthRequest finished for "+URL);
		return resp;
	}

	@Override
	public String getURL(Metadata func) throws AdapterException {		
		return URL;
	}

}
