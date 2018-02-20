/**
 * (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 */
package writebackadapter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
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
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.TableOptions;


/**
*	WriteBackAdapter Adapter.
*/
public class WriteBackAdapter extends Adapter{

	static Logger logger = LogManager.getLogger("WriteBackAdapter");
	private String name = null;
	private boolean alreadySent = false;
	private AdapterRowSet data;

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		
		PropertyGroup connectionInfo = new PropertyGroup("testParam","Test Parameters","Test Parameters");
		connectionInfo.addProperty(new PropertyEntry("name", "name"));
		
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "Test Credentials");
		credential.getUser().setDisplayName("Demo Username");
		credential.getPassword().setDisplayName("Demo Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}
	
	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version)
			throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		capbility.setCapability(AdapterCapability.CAP_SELECT);
		capbility.setCapability(AdapterCapability.CAP_INSERT);
		capbility.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capbility.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);		
		return capbility;
	}

	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC)
			throws AdapterException {
		logger.info("Creating connection to " + connectionInfo.getRemoteSourceName());		

		String username = "";
		String password = "";
		try {
			username = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getUser().getValue(), "UTF-8");
			password = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getPassword().getValue(), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1, e1.getMessage());
		}
		
		name = connectionInfo.getConnectionProperties().getPropertyEntry("name").getValue();
		
		//Let's prepare initial data.
		TableMetadata table = (TableMetadata) importMetadata("InMemoryTable");
		data = new AdapterRowSet(table.getColumns());
		AdapterRow row = data.newRow();
		row.setColumnValue(0, 1); 
		row.setColumnValue(1, "Hello " + name);
	}
		
	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		logger.info("browsing Metadata");		

		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		BrowseNode node = new BrowseNode("InMemoryTable", "InMemoryTable");
		node.setImportable(true);
		node.setExpandable(false);
		nodes.add(node);
		return nodes;
	}



	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		logger.info(String.format("importing metadata to HANA for %s", nodeId));		
	    
		List<Column> schema = new ArrayList<Column>();
		Column col1 = new Column("intColumn", DataType.INTEGER);
		col1.setNullable(true);	schema.add(col1);
		Column col2 = new Column("textColumn", DataType.VARCHAR, 256);
		col2.setNullable(true);
		schema.add(col2);
		TableMetadata table = new TableMetadata();
		table.setName(nodeId);
		table.setColumns(schema);
		return table;
	}

	@Override
	public void executeStatement(String sql,StatementInfo info) throws AdapterException {
		logger.info("Execute executeStatement [" + sql + "].");		
		alreadySent = false;
	}

	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
	    logger.info("Request to send data to hana");		
	    /**
		 * Currently this function gets until it returns a empty row set.
		 */
		if(alreadySent) return;
		/**
		 * copy the data from in memory table to response to hana.
		 */
		for(int i = 0; i < data.getRowCount(); i++){
			AdapterRow row = rows.newRow();
			AdapterRow row2copyFrom = data.getRow(i);
			for(int col=0; col< data.getColumns().size(); col++){
				row.setColumnValue(col, row2copyFrom.getColumnStringValue(col)); 
			}
		}
		alreadySent = true;
	}

	@Override
	public void executePreparedInsert(String sql, StatementInfo arg1)
			throws AdapterException {
       logger.info("Execute executePreparedInsert [" + sql + "].");		
	}

	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		for(int i = 0; i < rows.getRowCount(); i++){
			AdapterRow row = data.newRow();
			AdapterRow row2copyFrom = rows.getRow(i);
			for(int col=0; col< data.getColumns().size(); col++){
				row.setColumnValue(col, row2copyFrom.getColumnStringValue(col)); 
			}
		}
		return rows.getRowCount();
	}

	@Override
	public void executePreparedUpdate(String sql, StatementInfo info)
			throws AdapterException {
	   logger.info("Execute executePreparedUpdate [" + sql + "].");		
	}

	@Override
	public int executeUpdate(String sql, StatementInfo info)
			throws AdapterException {
		logger.info("Execute executeUpdate [" + sql + "].");		
		return 0;
	}
	

	@Override
	public void commitTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	

	@Override
	public void close() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize)
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
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription)
			throws AdapterException {
		return null;
	}

	
	@Override
	public void beginTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
		// TODO Auto-generated method stub
		
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
		
	@Override
	public void executeCall(FunctionMetadata metadata) throws AdapterException{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void validateCall(FunctionMetadata metadata) throws AdapterException{
		// TODO Auto-generated method stub
	}
	
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
	public void closeResultSet() throws AdapterException {
		// TODO Auto-generated method stub
		
	}
}
