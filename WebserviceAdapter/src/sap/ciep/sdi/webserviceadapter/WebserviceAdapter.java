/**
 * (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 */
package sap.ciep.sdi.webserviceadapter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;

import sap.ciep.sdi.webserviceadapter.plugins.SECEdgarMasterService;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.Property;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;


/**
*	WebserviceAdapter Adapter.
*/
public class WebserviceAdapter extends Adapter{

	static Logger logger = LogManager.getLogger("WebserviceAdapter");
	private String tableName;
//	private String connectionName;

	String username = "";
	String password = "";
	String hostAddr = "";
	HashMap<String,String> connProps = new HashMap<String,String>();
	
	/**
	 * Queue of records to return to HANA
	 */
	private LinkedList<WebserviceResponseRecord> recordQueue ;
	
	/**
	 * Timer for call duration
	 */
	private long startReqTime;
	
	/**
	 * List of webservices to provide.
	 * For convenience, it is declared as an inline array, but also accessible via the hasmap 'entries'
	 */
	protected static final WebserviceConfig[] configs = { new SECEdgarMasterService() };
	private static final HashMap<String,WebserviceConfig> entries = initEntries();

	/**
	 * Copies the array of webservices configurations into a hashmap for easier lookup 
	 */
	private static HashMap<String,WebserviceConfig> initEntries() {
		HashMap<String,WebserviceConfig> hm = new HashMap<String,WebserviceConfig>() ;
		for(WebserviceConfig cfg: configs){
			hm.put(cfg.getNodeName(), cfg);
		}
		return hm;
	}

	/**
	 * 
	 */
	private int fetchSize=1000;
	
	/**
	 * Returns the functions that the adapter provides
	 */
	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		
		for(WebserviceConfig cfg: configs){
			BrowseNode n=new BrowseNode(cfg.getNodeName(), cfg.getNodeName(), true);
			n.setExpandable(false);
			n.setImportable(true);
			n.setNodeType(cfg.getNodeType());
			nodes.add(n);
		}
		return nodes;
	}

	@Override
	public void validateCall(FunctionMetadata metadata) throws AdapterException{
		logger.debug("validateCall for "+metadata.getName());
		this.tableName = metadata.getName();	
		if(recordQueue!=null && !recordQueue.isEmpty()) 
			recordQueue.clear();
	}
			
	@Override
	public void executeCall(FunctionMetadata metadata) throws AdapterException{
		logger.debug("executeCall");
		startReqTime=System.currentTimeMillis();
		String url="";
		WebserviceConfig ws=entries.get(tableName);

		try {
			CloseableHttpResponse resp= ws.exec(metadata, connProps);
			url=ws.reqHandler.getURL(metadata);
			recordQueue=ws.handleResponse(resp, url);
		} catch (IOException | URISyntaxException e) {
			logger.error(e);
		}
		long t=System.currentTimeMillis() - startReqTime;
		logger.debug("call to '"+url+"' completed in "+t+" ms");
	}
	
	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
	   /**
	    * Currently this function gets until it returns a empty row set.
	    */
		if(recordQueue==null||recordQueue.isEmpty()) 
			return;
		
		logger.debug("in getNext, table="+tableName+"   queue size="+recordQueue.size()+" fetch="+fetchSize);
		
		int i=0;
		WebserviceResponseRecord rec;
		while (!recordQueue.isEmpty() && i<fetchSize){
			//logger.debug("Add row "+i);
			rec =recordQueue.poll();
			rec.appendTo(rows);
			i++;
		}

	}

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		
		PropertyGroup sourceParams = new PropertyGroup("WebserviceParameters", "Webservice Parameters", "Webservice Parameters");
		
		PropertyEntry pe = new PropertyEntry("host","Host and port", "Host and Port");
		pe.setIsRequired(true);
		sourceParams.addProperty(pe);
		
		for (WebserviceConfig c : configs){
			PropertyGroup pg = c.getRemoteSourceParameters();
			if (pg!=null)
				sourceParams.addProperty(pg);
		}
		
				
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "User Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);
	
		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(sourceParams);
		return rs;
	}
	
	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
	   
	   logger.debug("importMetadata");	

	   if (entries.containsKey(nodeId)){
		   return entries.get(nodeId).getMetadata();
	   }
		  
	   return null;
		
	}

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC)
			throws AdapterException {
			
		try {
			username = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getUser().getValue(), "UTF-8");
			password = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getPassword().getValue(), "UTF-8");
			connProps.put("username", username);
			connProps.put("password", password);
			
		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1, e1.getMessage());
		}
				
		for (Property p : connectionInfo.getConnectionProperties().getProperties()){
			if(p instanceof PropertyGroup){				
				for(Property p2: ((PropertyGroup)p).getProperties()){
					if(p2 instanceof PropertyEntry){
						connProps.put(p.getName()+"."+p2.getName(), ((PropertyEntry) p2).getValue());
					}
				}				
			}
			else if(p instanceof PropertyEntry){			
				connProps.put(p.getName(), ((PropertyEntry) p).getValue());
			}
		}
		
		logConnProps(connProps);
		//connectionName = connectionInfo.getConnectionProperties().getPropertyEntry("name").getValue();
	}



	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version)
			throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		capbility.setCapability(AdapterCapability.CAP_SELECT);
		capbility.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capbility.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		return capbility;
	}

	/**
	 * Lobs are stored in the response handler
	 */
	public int getLob(long lobId, byte[] bytes, int bufferSize)
			throws AdapterException {	
		logger.debug("getLob "+lobId);
		return entries.get(tableName).respHandler.getLob(lobId, bytes, bufferSize);
	}
	
	static public void logConnProps(HashMap<String,String> props){
		logger.debug("connProps: "+props.size());
		for(Entry<String, String> e : props.entrySet()){
			logger.debug("		"+e.getKey()+":"+e.getValue());
		}
	}

	/**
	 * Processes selects against virtual tables
	 */
	@Override
	public void executeStatement(String sql,StatementInfo info) throws AdapterException {
		//alreadySent = false;
		logger.debug("executeStatement "+ sql);
		SQLHelper helper = new SQLHelper(sql);
		this.tableName = helper.getTables().get(0).toUpperCase();	
		if(recordQueue!=null && !recordQueue.isEmpty()) 
			recordQueue.clear();
		logger.info("Read data from [" + tableName + "].");
		
		WebserviceConfig ws=entries.get(tableName);
		String url = null;
		try {
			CloseableHttpResponse resp= ws.exec(null, connProps);
			url=ws.reqHandler.getURL(null);
			recordQueue=ws.handleResponse(resp, url);
		} catch (IOException | URISyntaxException e) {
			logger.error(e);
		}
		long t=System.currentTimeMillis() - startReqTime;
		logger.debug("call to '"+url+"' completed in "+t+" ms");
		
	}

	
@Override
public void closeResultSet() throws AdapterException {
	// TODO Auto-generated method stub
	logger.debug("closeResultSet()");
	if(recordQueue!=null && !recordQueue.isEmpty()) 
		recordQueue.clear();
}	

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	// Unimplemented methods below

	@Override
	public void setNodesListFilter(RemoteObjectsFilter remoteObjectsFilter) throws AdapterException {
		// TODO Auto-generated method stub
    }
	

	@Override
	public Metadata getMetadataDetail(String nodeId) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableProcedure prepareCall(ProcedureMetadata metadata) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	@Override
	public void beginTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("close()");
	}

	@Override
	public void commitTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		return 0;
	}
	
	@Override
	public void rollbackTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void setFetchSize(int fetchSize) {
		
		logger.debug("setting fetch size to "+fetchSize);
		this.fetchSize=fetchSize;
	}
	
	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void executePreparedInsert(String arg0, StatementInfo arg1)
			throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1)
			throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int executeUpdate(String sql, StatementInfo info)
			throws AdapterException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public Metadata importMetadata(String nodeId,
			List<Parameter> dataprovisioningParameters) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public ParametersResponse queryParameters(String nodeId,
			List<Parameter> parametersValues) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public List<BrowseNode> loadTableDictionary(String lastUniqueName)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
}
