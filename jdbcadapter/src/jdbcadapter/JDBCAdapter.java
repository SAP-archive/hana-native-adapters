package jdbcadapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.LobCharset;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.UniqueKey;

/**
 * This is a sample adapter that connects to a database with jdbc driver.
 * You can modify driver and url to connect to any database system. currently it is pointing to SQL server
 */
public class JDBCAdapter extends Adapter{

	static Logger logger = LogManager.getLogger("JDBCAdapter");
	/**JDBC Driver**/
	static final String JDBC_DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	/**JDBC URL**/
	private String JDBC_URL = "jdbc:sqlserver://HOST;databaseName=DB_NAME";
	/**JDBC connections **/
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet resultSet = null;
	private ResultSet browseResultSet = null;
	

	/** Node user is browsing **/
	private String nodeID = null; 
	/** SQL Statement executed by the user **/
	private String sql = null; 
	/** Browse node offset**/
	private int browseOffset = 0;
	private int fetchSize;
	private String database;
	
	private HashMap<Long, InputStream> blobHandle;
	private HashMap<Long, Reader> clobHandle;
	
	/**
	 * UI Information.
	 * Provide variables that user need to provide value required to create
	 * a connection to underlying database system. For example:
	 * Username, password, database name, etc.
	 * Note this method is called once and cached in a table.
	 * You can invoke the stored procedure to get the list
	 * 		call "PUBLIC"."DATAPROV_ADAPTER_SERVICE"('ui' ,'JDBCAdapter', '', ?, ?, ?)
	 */
	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup connectionInfo = new PropertyGroup("connectionInfo","Connection","Connection");
		connectionInfo.addProperty(new PropertyEntry("HOST", "Host"));
		connectionInfo.addProperty(new PropertyEntry("DB_NAME", "DataBase Name"));
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "JDBC Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);
		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}


	/**
	 * The method is called once the user provides the input in UI
	 * If the connection can not be made to underlying system it should throw
	 * an exception explaining the problem otherwise set up the connection
	 * so beginTran..EndTran can be called.
	 * 
	 * @param connectionInfo is a map containing the values provided by the user
	 * The key for the map is obtained from UIPropertyEntry send in getUI method
	 */
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC)
			throws AdapterException {
		
		String username = "";
		String password = "";
		try {
			username = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getUser().getValue(), "UTF-8");
			password = new String(connectionInfo.getCredentialProperties().getCredentialEntry("credential").getPassword().getValue(), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1, e1.getMessage());
		}
		
		String host = connectionInfo.getConnectionProperties().getPropertyEntry("HOST").getValue();
		database = connectionInfo.getConnectionProperties().getPropertyEntry("DB_NAME").getValue();
		
		String jdbc_jar = "lib/sqljdbc.jar";

		if(username.isEmpty() || password.isEmpty() )
			throw new AdapterException("Credentials can not be empty");
		
		File file = new File(jdbc_jar);
		if(!file.exists())
			throw new AdapterException("No sqljdbc.jar is present at " + jdbc_jar);
		
		blobHandle = new HashMap<Long, InputStream>();
		clobHandle = new HashMap<Long, Reader>();
		try {
			
			URL u = new URL("jar:file:" + jdbc_jar + "!/");
			URLClassLoader ucl = new URLClassLoader(new URL[] { u });
			
			Driver d = (Driver)Class.forName(JDBC_DRIVER_NAME, true, ucl).newInstance();
			DriverManager.registerDriver(new DriverDelegator(d));
			String replaced = JDBC_URL.replaceAll("HOST", host);
			replaced = replaced.replaceAll("DB_NAME", database);
			conn = DriverManager.getConnection(replaced,username,password);
			stmt = conn.createStatement();
		} catch (Exception e) {
			String error = e.getMessage();
			if(e.getMessage() == null)
				error = e.toString();
			throw new AdapterException(error);
		} 
	}

	
	@Override
	public void close() throws AdapterException {
		/**
		 * Cleanup connections, thread and all the element your adapter is using.
		 */
		try {
			if(resultSet != null)
				resultSet.close();
			if(browseResultSet != null)
				browseResultSet.close();
			conn.close();
		} catch (SQLException e) {
			throw new AdapterException(e.getMessage());
		}
	}

	@Override
	public void beginTransaction() throws AdapterException {
	}

	/**
	 * For each queries ,the executeStatement() is called first and it is followed by
	 * one or more of getNext() calls.
	 * This method comes with a rowList already created and the columns is target columns.
	 */
	@Override
	public void getNext(AdapterRowSet rowList) throws AdapterException{
		try {
//			blobHandle.clear();
			/*
			 * This method will be called multiple times based on what you return.
			 * Make sure you honor the fetchSize requirement by only sending that many rows.
			 */
			int rowNum = 0;
			while(resultSet.next())
			{
				/*
				 * For each row in resultSet, create a AdapterRow 
				 * Set the values for each columns 
				 */
				rowList.newRow();
				List<Column>columns = rowList.getColumns();
				for(int i=0; i < columns.size(); i++){
					//Always add to last row
					setValue(rowList.getRow(rowList.getRowCount()-1), columns.get(i), resultSet, i, rowNum);
					/**
					 * Note in case of lob datatypes, if you put lob columns here let's say you send 10 rows with
					 * lob id inside.
					 * Framework will call getLob to get all the lob data before fetching the next batch of rows
					 * using getNext.
					 */
				}
				rowNum++;
				if(rowNum == fetchSize) //Do not add more than fetchSize.
					break;
			}
			
		} catch (SQLException e) {
			throw new AdapterException(e.getMessage());
		}
	}


	@Override
	public int getLob(long lobHandleId, byte[] bytes, int bufferSize)
			throws AdapterException {
		try{
			int readBytes;
			if(blobHandle.containsKey(lobHandleId)){
				InputStream is = blobHandle.get(lobHandleId);
				readBytes = is.read(bytes); // is.read(bytes, offSet, bufferSize);
				if(readBytes < 0)
					return 0; //We can not send 0 bytes array
				return readBytes;	
			}else if(clobHandle.containsKey(lobHandleId)){
				Reader is = clobHandle.get(lobHandleId);
				char[] buffer = new char[bufferSize];
				readBytes = is.read(buffer, 0, bufferSize);
				if(readBytes < 0)
					return 0; //We can not send 0 bytes array
				ByteBuffer bb = ByteBuffer.wrap(bytes); 
				CharBuffer cb = CharBuffer.wrap(buffer,0,readBytes);
				ByteBuffer result = Charset.forName("UTF-8").encode(cb);
				bb.put(result);
				return result.position();					
			}else{
				return 0;
			}
		} catch (IOException e) {
			throw new AdapterException(e.getLocalizedMessage());
		}

	}

	/**
	 * When the user is browsing, this method is called first to set the nodeId
	 * to be expanded.
	 */
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
		this.nodeID = nodeId;
		browseOffset = 0;
		
		if (browseResultSet != null) {
			try {
				browseResultSet.close();
			} catch (SQLException e) {
				logger.warn("Failed to close ResultSet.", e);
			}
		}
		
		if(this.nodeID != null){
			try {
 				DatabaseMetaData mds = conn.getMetaData();
				browseResultSet = mds.getTables(nodeId, null, "%", null);
			} catch (SQLException e) {
				throw new AdapterException(e.getMessage());
			}
		}
	}

	/**
	 * For a Database the browse would something like below.
	 * -database
	 * 		-owner
	 * 			-table1
	 * 			-table2
	 * 		-owner
	 * 			-table1
	 * 			-table2
	 * Since putting the whole tree in memory is expensive. It is rather
	 * preferred that you create the tree dynamincally for each request.
	 * 
	 * You can make the nodeId unique by using database.owner.table1
	 * The dot separation gives an example on how to keep track of level. 
	 */
	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		/*
		 * We will do a trick here. If node id is null, that means user is browsing at the root.
		 * So return the current database name else return all tables belonging to this database.
		 */
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		if(this.nodeID == null)
		{
			BrowseNode node = new BrowseNode(database, database);
			/**This is a root node, we want to be expandable and not importable **/
			node.setImportable(false); 
			node.setExpandable(true);
			nodes.add(node);
		}
		else{
			try {
				while(browseResultSet.next()){
					//Now the tricky part of keeping track of offset.
					//Since in this method we only require count amount of nodes.
					//keep track of the offset that you have already returned.
					browseOffset++;
					//This is list of tables for owner dbo.
					BrowseNode node = new BrowseNode(browseResultSet.getString(3), browseResultSet.getString(3));
					/**These are table nodes, we want them to be importable and not expandable**/
					node.setImportable(true);
					node.setExpandable(false);
					nodes.add(node);
					if(browseOffset == fetchSize)
						break;
				}
			} catch (SQLException e) {
				throw new AdapterException(e.getMessage());
			}
		}
		return nodes;
	}

	/**
	 * Helper Method
	 * Call the appropiate method on the row the set the column value.
	 * You can extend it to other datatypes.
	 */
	private void setValue(AdapterRow row, Column column,ResultSet rs,  int colIndex, int rowIndex) throws AdapterException, SQLException {
		Calendar cal = Calendar.getInstance();
		Long lobId = (long) (rowIndex * this.fetchSize+  colIndex);
		switch(column.getDataType()){
		case INTEGER:
			row.setColumnValue(colIndex, rs.getInt(colIndex+1));
			break;
		case BIGINT:
			row.setColumnValue(colIndex, rs.getLong(colIndex+1));
			break;
		case DOUBLE:
			row.setColumnValue(colIndex, rs.getDouble(colIndex+1));
			break;
		case DECIMAL:
			row.setColumnValue(colIndex, rs.getBigDecimal(colIndex+1));
			break;	
		case VARBINARY:
			row.setColumnValue(colIndex, rs.getBytes(colIndex+1));
			break;	
		case DATE:
			Date date = rs.getDate(colIndex+1);
			if(date == null){
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTime(date);
			row.setColumnValue(colIndex, new Timestamp(cal));
			break;
		case SECONDDATE:
		case TIME:
			Time time = rs.getTime(colIndex+1);
			if(time == null){
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTime(time);
			row.setColumnValue(colIndex, new Timestamp(cal));		
			break;
		case TIMESTAMP:
			java.sql.Timestamp timeStamp = rs.getTimestamp(colIndex+1);
			if(timeStamp == null){
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTimeInMillis(timeStamp.getTime());
			row.setColumnValue(colIndex, new Timestamp(cal));
			break;
		case BLOB:
			Blob blob1 = rs.getBlob(colIndex+1);
			blobHandle.put(lobId,blob1.getBinaryStream()); 
			row.setColumnLobIdValue(colIndex, lobId, LobCharset.ASCII);
			break;
		case CLOB:
		case NCLOB:
			Clob clob = rs.getClob(colIndex+1);
			if(clob == null){
				row.setColumnLobIdValue(colIndex, 0, LobCharset.ASCII); 
				column.setNullable(true); ///TODO 
			}
			else{
				clobHandle.put(lobId, clob.getCharacterStream());//clob.getAsciiStream());
				row.setColumnLobIdValue(colIndex, lobId, LobCharset.UTF_8);
			}
			break;
//		case NCLOB:
//			//We need to know which row and which col this blob belongs to so we can fetch it.
//			NClob blob = rs.getNClob(colIndex+1);
//			clobHandle.put(lobId, blob.getCharacterStream()); //rs.getUnicodeStream(colIndex+1));//  
//			row.setColumnValue(colIndex, lobId);
//			break;
		default:
			row.setColumnValue(colIndex, rs.getString(colIndex+1));
		}
	}

	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		TableMetadata metas = new TableMetadata();
		metas.setName(nodeId);
		metas.setPhysicalName(nodeId);
		try{
			ResultSet rsColumns = null;
			DatabaseMetaData meta = conn.getMetaData();
			//catalog, schemaPattern, tableNamePatter, types
			rsColumns = meta.getColumns(null, null, nodeId , null);
			List<Column> cols = new ArrayList<Column>();
			while (rsColumns.next()) {
				Column col = getColumn(rsColumns);
				cols.add(col);
			}
			metas.setColumns(cols);
			metas.setUniqueKeys(getUniqueKeys(nodeId));
			setPrimaryFlagForColumns(metas);
			
		}catch(SQLException e){
			throw new AdapterException(e.getMessage());
		}
		return metas;
	}


	private Column getColumn(ResultSet rsColumns) throws SQLException,
			AdapterException {
		String columnName = rsColumns.getString("COLUMN_NAME");
		int    columnType = rsColumns.getInt("DATA_TYPE");
		String typeName = rsColumns.getString("TYPE_NAME");
		if (typeName.compareTo("date") == 0)
			columnType = 91;
		else if (typeName.compareTo("time") == 0)
			columnType = 92;
		else if (typeName.compareTo("datetime2") == 0)
			columnType = 93;
		else if (typeName.compareTo("text") == 0)
			columnType = java.sql.Types.CLOB;
		else if (typeName.compareTo("ntext") == 0)
			columnType = java.sql.Types.NCLOB;
		else if (typeName.compareTo("image") == 0)
			columnType = java.sql.Types.BLOB;
		else if (typeName.compareTo("nvarchar") == 0)
			columnType = java.sql.Types.NVARCHAR;
		else if (typeName.compareTo("nchar") == 0)
			columnType = java.sql.Types.NCHAR;
//@Bug Index Server Changes peding. FIXME uncomment when IS is fixed
		else if (typeName.compareTo("smalldatetime") == 0)
			//columnType = -100;  //Since java does not have this, we create our own version.
			columnType = java.sql.Types.TIMESTAMP;

		int size = rsColumns.getInt("COLUMN_SIZE");
		int nullable = rsColumns.getInt("NULLABLE");

		Column col = new Column(columnName, getAdapterDataType(columnType));
		col.setLength(size);
		col.setNullable(nullable == DatabaseMetaData.columnNullable);
		if(getAdapterDataType(columnType) == DataType.DECIMAL){
			col.setPrecision(size);
			col.setScale(rsColumns.getInt("DECIMAL_DIGITS"));
		}
		return col;
	}


	private void setPrimaryFlagForColumns(TableMetadata metas) {
		List<Column> columns = metas.getColumns();
		List<UniqueKey> keys = metas.getUniqueKeys();
		for(UniqueKey key: keys){
			List<String> columnNames = key.getColumnNames();
			for(Column column: columns)
				if(columnNames.contains(column.getName()))
						column.setPrimaryKey(true);
		}
	}


	public DataType getAdapterDataType(int dbType) throws AdapterException{
		switch (dbType){
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return DataType.VARCHAR;

		case java.sql.Types.NCHAR:
		case java.sql.Types.NVARCHAR:
			return DataType.NVARCHAR;
			
		case java.sql.Types.INTEGER:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.TINYINT:
			return DataType.INTEGER;

		case java.sql.Types.BIGINT:
			return DataType.BIGINT;

		case java.sql.Types.NUMERIC:
		case java.sql.Types.DECIMAL:
			return DataType.DECIMAL;

		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:
			return DataType.REAL;
			
		case java.sql.Types.DOUBLE:
			return DataType.DOUBLE;

		case java.sql.Types.TIMESTAMP:
			return DataType.TIMESTAMP;

		case java.sql.Types.DATE:
			return DataType.DATE;

		case java.sql.Types.TIME:
			return DataType.TIME;

		case java.sql.Types.BLOB:
			return DataType.BLOB;
		
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.CLOB:
			return DataType.CLOB;
			
		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.NCLOB:
			return DataType.NCLOB;
		
		case java.sql.Types.BINARY:
			return DataType.VARBINARY;
		case -100:
			return DataType.SECONDDATE;
		default:
			return DataType.VARCHAR;
		}
	}



	@Override
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription)
					throws AdapterException {
		return null;
	}

	@Override
	public void commitTransaction() throws AdapterException {
	}


	@Override
	public void rollbackTransaction() throws AdapterException {
	}


	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		return 0;
	}
	/**
	 * When an user invokes a select query, based on your adapter capabilities
	 * You get a SQL that your adapter can use.
	 * If you support joins and other capabilties, you may need to parse this sql
	 * and rewrite it to match your system.
	 */
	@Override
	public void executeStatement(String sql, StatementInfo info) throws AdapterException {
		/**
		 *Since we have a simple adapter with no push down.
		 *So we will directly use it. 
		 *If you do support pushdown you need to parse the sql,
		 *figure out the columns being returned and save a copy to be used in getNext.
		 */

		blobHandle.clear();
		clobHandle.clear();

		this.sql = SQLRewriter.rewriteSQL(sql);
			
			try {
				conn.setAutoCommit(false);
				stmt.setFetchSize(fetchSize);//So that fetch size work.
				/*if(resultSet!=null)
					resultSet.close();*/ //invalid instance use bug. TODO
				logger.trace(this.sql);
				resultSet = stmt.executeQuery("use "+database+";" + this.sql);
			} catch (SQLException e) {
				throw new AdapterException(e.getMessage());
			}
			
/*		}
		catch ( Exception e ) {
			throw new AdapterException(e.getLocalizedMessage());
		}*/

	}
	/**
	 * Adapter capabilities defines what push down capabilities this adapter has.
	 * Does your underlying system handle joins, insert, etc?
	 * You need to add each of the capability for this adapter in this method.
	 * This method is called at the initial registration of this adapter.
	 * 
	 * Note the response will be cached and if you decide to change you will need to restart adapter.
	 */
	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version)
			throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		List<AdapterCapability> capibilities = new ArrayList<AdapterCapability>();
		if( System.getenv("DP_AGENT_DIR") == null || System.getenv("CAPS_INI") == null ) {
			capibilities.add(AdapterCapability.CAP_ALTER_TAB_WITH_ADD );
			capibilities.add(AdapterCapability.CAP_ALTER_TAB_WITH_DROP );
			capibilities.add(AdapterCapability.CAP_WINDOWING_FUNC);
			capibilities.add(AdapterCapability.CAP_BI_ADD);
			capibilities.add(AdapterCapability.CAP_BIGINT_BIND);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
			capibilities.add(AdapterCapability.CAP_INSERT_SELECT_ORDERBY);
			capibilities.add(AdapterCapability.CAP_DELETE);

			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_PROJ);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_WHERE);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_INNER_JOIN);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_INNER_JOIN);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_LEFT_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_LEFT_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_FULL_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_FULL_OUTER_JOIN);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_GROUPBY);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_GROUPBY);
			capibilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_ORDERBY);
			capibilities.add(AdapterCapability.CAP_EXPR_IN_ORDERBY);
			capibilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_ORDERBY);
			capibilities.add(AdapterCapability.CAP_SELECT);
			capibilities.add(AdapterCapability.CAP_SCALAR_FUNCTIONS_NEED_ARGUMENT_CHECK);
			capibilities.add(AdapterCapability.CAP_NONEQUAL_COMPARISON);
			capibilities.add(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);	
			capibilities.add(AdapterCapability.CAP_PROJECT);	

			capibilities.add(AdapterCapability.CAP_LIKE);
			capibilities.add(AdapterCapability.CAP_GROUPBY);	
			capibilities.add(AdapterCapability.CAP_ORDERBY);	
			capibilities.add(AdapterCapability.CAP_AGGREGATES);	
			capibilities.add(AdapterCapability.CAP_AGGREGATE_COLNAME);	
			capibilities.add(AdapterCapability.CAP_JOINS);	
			capibilities.add(AdapterCapability.CAP_JOINS_OUTER);	
			capibilities.add(AdapterCapability.CAP_AND);	
			capibilities.add(AdapterCapability.CAP_OR);	
			capibilities.add(AdapterCapability.CAP_BETWEEN);
			capibilities.add(AdapterCapability.CAP_IN);
			//capibilities.add(AdapterCapability.CAP_TABLE_CAP);	
			//capibilities.add(AdapterCapability.CAP_COLUMN_CAP);	
			capibilities.add(AdapterCapability.CAP_BI_SUBSTR);	
			capibilities.add(AdapterCapability.CAP_BI_MOD);	
			capibilities.add(AdapterCapability.CAP_AGGREGATES);	
	    	capibilities.add(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		}
		else {

			String sFileName=System.getenv("DP_AGENT_DIR") + "/configuration/" + System.getenv("CAPS_INI");

			try {
				Properties prop = new Properties();
				InputStream input = null;
				input = new FileInputStream(sFileName);
				prop.load(input);

				Enumeration e = prop.propertyNames();

				while (e.hasMoreElements()) {
					String key = (String) e.nextElement();
					int value =  Integer.parseInt(prop.getProperty(key));				 
					capibilities.add(AdapterCapability.valueOf(value));
				}
			} catch (IOException e) {
				throw new AdapterException(e,e.getLocalizedMessage());
			}	    	 
		}			


		capbility.setCapabilities(capibilities);
		return capbility;
	}
	
	private List<UniqueKey> getUniqueKeys(String tableName) throws SQLException{
		ArrayList<UniqueKey> uniqueKeys = new ArrayList<UniqueKey>();
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet set = meta.getPrimaryKeys(conn.getCatalog(), null, tableName);
		HashMap<String,List<String>> map = new HashMap<String,List<String>>();
	    while (set.next()) {
	    	String indexName = set.getString("PK_NAME");
	    	if (indexName==null)
	    		continue;
	    	String fieldName = set.getString("COLUMN_NAME");
	    	if (!map.containsKey(indexName))
    			map.put(indexName, new ArrayList<String>());
	    	map.get(indexName).add(fieldName);
	    }
	    for(String key: map.keySet())
	    {
	    	UniqueKey uniqueKey = new UniqueKey(key, map.get(key));
	    	uniqueKey.setPrimary(true);
	    	uniqueKeys.add(uniqueKey);
	    }
	    return uniqueKeys;
	}


	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void executePreparedInsert(String sql, StatementInfo info)
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
	public void executePreparedUpdate(String sql, StatementInfo info)
			throws AdapterException {
		// TODO Auto-generated method stub
	}

	@Override
        public Metadata importMetadata(String nodeId, List<Parameter> dataprovisioningParameters) throws AdapterException {
                return null;
        }

	@Override
        public ParametersResponse queryParameters(String nodeId, List<Parameter> parametersValues) throws AdapterException {
                return null;
        }


	@Override
	public List<BrowseNode> loadTableDictionary(String lastUniqueName) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public DataDictionary loadColumnsDictionary() {
		// TODO Auto-generated method stub
		return null;
	}	
}

class DriverDelegator implements Driver {
    private Driver driver;
    
    DriverDelegator(Driver d) { 
    	this.driver = d;
    }
    public boolean acceptsURL(String u) throws SQLException {
        return this.driver.acceptsURL(u);
    }
    
	@Override
	public Connection connect(String u, Properties p) throws SQLException {
		return this.driver.connect(u, p);
	}
	@Override
	public int getMajorVersion() {
		return this.driver.getMajorVersion();
	}
	@Override
	public int getMinorVersion() {
		return this.driver.getMinorVersion();
	}
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
			throws SQLException {
		return this.driver.getPropertyInfo(arg0, arg1);
	}
	@Override
	public boolean jdbcCompliant() {
		return this.driver.jdbcCompliant();
	}
	/**
	 * Allow on 1.7 java 
	 * @return
	 * @throws SQLFeatureNotSupportedException
	 */
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
          throw new SQLFeatureNotSupportedException();
    }

    
}
