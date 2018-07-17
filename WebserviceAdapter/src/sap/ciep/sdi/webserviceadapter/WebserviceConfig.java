package sap.ciep.sdi.webserviceadapter;

import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterLang;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.NodeType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;

import com.sap.hana.dp.adapter.sdk.AdapterException;

/**
 * Configuration of the various webservice capabilities 
 * @author I063909
 *
 */
public abstract class WebserviceConfig {

	/**
	 * Name of the webservice
	 */
	protected String name;
	
	//description
	protected String description;
		
	protected WebserviceRequestHandler  reqHandler;
	protected WebserviceResponseHandler respHandler;

	public WebserviceConfig(String _name, String _description, WebserviceRequestHandler _reqHandler, WebserviceResponseHandler _respHandler  ){
		name=_name;
		description=_description;
		reqHandler=_reqHandler;
		respHandler=_respHandler;
	}
	
	public abstract Metadata getMetadata() throws AdapterException;
	
	public String getNodeName(){
		return name;
	}
	
	/**
	 * Return the node type (table, procedure or function)
	 * @return
	 */
	public abstract NodeType getNodeType();
	
	/**
	 * Shorthand to access the request handler defined for this webservice
	 * @param func
	 * @return
	 * @throws ClientProtocolException
	 * @throws AdapterException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public CloseableHttpResponse exec(FunctionMetadata func, HashMap<String,String> connProps) throws ClientProtocolException, AdapterException, IOException, URISyntaxException{
		return reqHandler.exec(func, connProps);
	}
	
	/**
	 * Shorthand to access the response handler for this webservice
	 * @param r
	 * @param url
	 * @return
	 * @throws IOException 
	 * @throws AdapterException 
	 * @throws ParseException 
	 */
	public LinkedList<WebserviceResponseRecord> handleResponse( CloseableHttpResponse r, String url) throws ParseException, AdapterException, IOException{
		return respHandler.handleResponse(r, url);
	}
	
	/**
	 * Helps to define columns in one line
	 * @param name
	 * @param d
	 * @param length
	 * @param desc
	 * @return
	 */
	protected static Column buildColumn(String name, DataType d, int length, String desc){
		Column c =new Column(name,  d ) ;
		if(length!=0)
			c.setLength(length);
		if(desc!=null)
			c.addDescription(AdapterLang.ENGLISH, desc);
		
		return c;
	}
	
	public PropertyGroup getRemoteSourceParameters(){
		return null;
	}
			
}
