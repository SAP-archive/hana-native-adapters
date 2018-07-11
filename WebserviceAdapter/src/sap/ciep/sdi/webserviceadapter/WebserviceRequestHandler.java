package sap.ciep.sdi.webserviceadapter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Metadata;

@SuppressWarnings("restriction")
public abstract class WebserviceRequestHandler {
	protected static Logger logger = LogManager.getLogger("WebserviceRequestHandler");
	
	public final static int MAX_CONNECTIONS_PER_ROUTE=5;
	public final static int SOCKET_TIMEOUT=60000;
	public final static int CONNECT_TIMEOUT=3000;
	public final static int CONN_REQUEST_TIMEOUT=3000;
	
	/**
	 * This function performs the request and returns the response
	 * @param func metadata, as sent by the adapter
	 * @param connProps 
	 * @return
	 * @throws AdapterException
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public abstract CloseableHttpResponse exec(Metadata func, HashMap<String, String> connProps) throws AdapterException, ClientProtocolException, IOException, URISyntaxException;
	
	private static CloseableHttpClient cli;
	
	/**
	 * Returns the default HTTP client with 
	 * @return
	 */
	public static CloseableHttpClient getDefaultHttpClient(){
		if (cli==null)
			cli=HttpClientBuilder.create()
				.useSystemProperties()
				.setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
				.setMaxConnPerRoute(MAX_CONNECTIONS_PER_ROUTE)
				.setDefaultRequestConfig(getDefaultRequestConfig())
				.build();
		return cli;
	}

	
	/**
	 * Returns a url as a String
	 * @param func
	 * @return
	 * @throws AdapterException 
	*/
	public abstract String getURL(Metadata func) throws AdapterException;
	
	
	private static RequestConfig getDefaultRequestConfig(){
		return RequestConfig.custom()
				.setConnectionRequestTimeout(CONN_REQUEST_TIMEOUT)
				.setConnectTimeout(CONNECT_TIMEOUT)
				.setSocketTimeout(SOCKET_TIMEOUT)
				.build();
	}
}
