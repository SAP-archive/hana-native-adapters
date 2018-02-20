/**
 * (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 */
package demoadapter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.NodeType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.ParameterType;
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
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.ScalarParameter;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.TableParameter;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;


/**
 *	DemoAdapter Adapter.
 */
public class DemoAdapter extends Adapter{

	static Logger logger = LogManager.getLogger("DemoAdapter");
	private String name = null;
	private boolean alreadySent = false;
	private String nodeId = null;
	private String tableName;
	private FunctionMetadata demoFunction;

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
	}


	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		if (nodeId == null || nodeId.isEmpty() ){
			BrowseNode table = new BrowseNode("table", "Tables");
			table.setExpandable(true);
			nodes.add(table);
			BrowseNode function = new BrowseNode("function", "Functions");
			function.setExpandable(true);
			nodes.add(function);
			BrowseNode procedure = new BrowseNode("procedure", "Procedures");
			procedure.setExpandable(true);
			nodes.add(procedure);
		}else if (nodeId.compareTo("table") == 0){
			BrowseNode node = new BrowseNode("DemoTable", "Demo Table");
			node.setImportable(true);
			node.setExpandable(false);
			nodes.add(node);
		}else if (nodeId.compareTo("function") == 0){
			BrowseNode node = new BrowseNode("DemoFunction", "Demo Sum Function");
			node.setImportable(true);
			node.setExpandable(false);
			node.setNodeType(NodeType.FUNCTION);
			nodes.add(node);
		}else if (nodeId.compareTo("procedure") == 0){			
			BrowseNode node = new BrowseNode("DemoProcedure", "Demo Procedure");
			node.setImportable(true);
			node.setExpandable(false);
			node.setNodeType(NodeType.PROCEDURE);
			nodes.add(node);
		}
		return nodes;
	}



	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		logger.info(String.format("importing metadata to HANA for %s", nodeId));		
		if(nodeId.compareTo("DemoTable") == 0){
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
		}else if(nodeId.compareTo("DemoFunction") == 0){
			FunctionMetadata fm = new FunctionMetadata("DemoFunction");
			fm.addConfiguration("test","some attributes");

			fm.addScalarParameter(ParameterType.PARAM_IN,new Column("A", DataType.INTEGER));
			fm.addScalarParameter(ParameterType.PARAM_IN,new Column("B", DataType.INTEGER));

			TableParameter tp = new TableParameter(ParameterType.PARAM_RETURN);
			tp.addColumn(new Column("SUM", DataType.INTEGER));
			tp.addColumn(new Column("TEXT", DataType.NVARCHAR, 255));
			fm.addTableParameter(tp);
			return fm;
		}else if(nodeId.compareTo("DemoProcedure") == 0){
			ProcedureMetadata fm = new ProcedureMetadata("DemoProcedure");
			fm.addScalarParameter(ParameterType.PARAM_IN,new Column("intParameter", DataType.INTEGER));

			TableParameter tableParameter = new TableParameter(ParameterType.PARAM_OUT);
			tableParameter.addColumn(new Column("userInput", DataType.INTEGER));
			tableParameter.addColumn(new Column("NVARCHAR_COLUMN", DataType.NVARCHAR, 2000));
			fm.addTableParameter(tableParameter);

			return fm;
		}else{
			return null;
		}
	}

	@Override
	public void executeStatement(String sql,StatementInfo info) throws AdapterException {
		logger.info("Execute executeStatement [" + sql + "].");		
		SQLHelper helper = new SQLHelper(sql);
		tableName = helper.getTables().get(0);
		logger.info("Read data from [" + tableName + "].");		
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
		if(tableName.compareToIgnoreCase("demotable") == 0){
			AdapterRow row = rows.newRow();
			row.setColumnValue(0, 1); 
			row.setColumnValue(1, "Hello User " + name);
		}else if(tableName.compareToIgnoreCase("demofunction") == 0){
			String value = ((ScalarParameter)demoFunction.getParameter(0)).getValue();
			if(value == null || value.isEmpty())
				throw new AdapterException("Expected an integer value for parameter A");
			int aValue = Integer.parseInt(value);
			value = ((ScalarParameter)demoFunction.getParameter(1)).getValue();
			if(value == null || value.isEmpty())
				throw new AdapterException("Expected an integer value for parameter B");
			int bValue = Integer.parseInt(value);


			AdapterRow row = rows.newRow();
			row.setColumnValue(0, (aValue + bValue)); 
			row.setColumnValue(1, String.format("The sum of %s + %s",aValue, bValue));
		}else{
			
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
		return 0;
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
		this.nodeId = nodeId;
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
		//This will be called for DemoFunction.
		this.demoFunction = metadata;
	}

	@Override
	public void validateCall(FunctionMetadata metadata) throws AdapterException{
		this.tableName = metadata.getName();
		alreadySent = false;
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
		//This is called for DemoProcedure
		return new DemoProcedureCall(metadata);
	}

	@Override
	public void closeResultSet() throws AdapterException {
		// TODO Auto-generated method stub

	}

}
