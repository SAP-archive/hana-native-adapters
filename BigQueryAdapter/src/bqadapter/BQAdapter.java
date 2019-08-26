/**
 * (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package bqadapter;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Types;
import java.sql.Time;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.sap.hana.dp.adapter.sdk.AdapterCDC;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.LobCharset;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterStatistics;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.LatencyTicketSpecification;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;

/**
 * SAP BigQuery Adapter.
 */
public class BQAdapter extends AdapterCDC {
	static Logger logger = LogManager.getLogger(BQAdapter.class);
	
	static private Connection conn = null;
	static private String databaseName;
	static String projectID, catalogID, dataSetID;
	static String OAuth_email,OAuth_key_path, jdbcjar;
	static String propFile = "";

	private int fetchsize;
	private String browseNodeId;
	//private int metaOffset;
	private ResultSet results;
	private PreparedStatement pstmt;
	
	static boolean driverLoaded = false;
	static private AdapterRowSet data;
	
	private Hashtable<Long, byte[]> lobindex = new Hashtable<Long, byte[]>();
	private Hashtable<Long, Integer> lobindexlastread = new Hashtable<Long, Integer>();
		
	@Override
	public void beginTransaction() throws AdapterException {
		// TODO Auto-generated method stub
	}
	
		

	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		boolean isImportable = false;
		boolean isExpandable = false;
		ResultSet rs;
		
		try {
			if (this.browseNodeId == null) {
				isImportable = true;
				rs = getTablesForDatabase(BQAdapter.databaseName);
											
				while (rs.next()) {
					String nodeName = rs.getString(1);  //project name
					String node2 = rs.getString(2);   //dataset id
					String node3 = rs.getString(3);   //table name
			
					BrowseNode node = new BrowseNode(node3, node3);
					node.setImportable(isImportable);
					node.setExpandable(isExpandable);					
					nodes.add(node);
				}

			} else {
				logger.warn( "Provided browseNodeId: "+ this.browseNodeId);
			}
		} catch (SQLException e) {
			throw new AdapterException(e);
		}
		return nodes;
	}

	private ResultSet getTablesForDatabase(String database) throws SQLException {
		String query = "SELECT * FROM `"+BQAdapter.catalogID+"`."+BQAdapter.dataSetID+".INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME"; //LIMIT 10 OFFSET 10";
		PreparedStatement st = BQAdapter.conn.prepareStatement(query);		
		logger.debug("### BigQuery Query: "+query);
		ResultSet rs = st.executeQuery();
		return rs;
	}

	@Override
	public void close() throws AdapterException {
		logger.info("### BQAdapter close");
		try {
			lobindex.clear();
			lobindexlastread.clear();
			if (results != null) {
				results.close();
				results = null;
			}

			if (conn != null) {
				conn.close();
				conn = null;
			}

			logger.debug("Closed the connection");
		} catch (SQLException e) {
			throw new AdapterException(e);
		}
	}

	@Override
	public void commitTransaction() throws AdapterException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capability = new Capabilities<AdapterCapability>();
		
		//Capablities are set based on Hana's SDA BigQuery capabilities; False prefix indicates capabilites 
		// set to false in Hana SDA Bigquery properties
		//capability.setCapability(AdapterCapability.CAP_AGGREGATES_COLNAME);
		//false	capability.setCapability(AdapterCapability.CAP_CRT_TEMP_TABLES);
		//false	capability.setCapability(AdapterCapability.CAP_GROUPING_SETS);
		//false	capability.setCapability(AdapterCapability.CAP_INSERT);
		//false capability.setCapability(AdapterCapability.CAP_OFFSET_ARG);
		//false capability.setCapability(AdapterCapability.CAP_ORDERBY_NULLS_ORDERING);
		//false capability.setCapability(AdapterCapability.CAP_PARAM_FUNCTION_SUBSTITUTION);
		//false capability.setCapability(AdapterCapability.CAP_TABLE_CAP);
		//capability.setCapability(AdapterCapability.CAP_TOP);
		//false capability.setCapability(AdapterCapability.CAP_TOP_UNDER_UNION);
		//false	capability.setCapability(AdapterCapability.CAP_TRUNCATE_TABLE);
		//false	capability.setCapability(AdapterCapability.CAP_UNIONALL);
		//capability.setCapability(AdapterCapability.CAP_SUBQUERY_DELETE);
		
		/*
		 capability.setCapability(AdapterCapability.CAP_AGGREGATES);
		 

		capability.setCapability(AdapterCapability.CAP_AGGR_STDDEV);
		capability.setCapability(AdapterCapability.CAP_AND);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_ARRAY_FETCH);
		capability.setCapability(AdapterCapability.CAP_BETWEEN);
		capability.setCapability(AdapterCapability.CAP_CASE_EXPRESSION);
		capability.setCapability(AdapterCapability.CAP_COLUMN_CAP);
		capability.setCapability(AdapterCapability.CAP_CORRELATION_IN_UPDATE);
	
		capability.setCapability(AdapterCapability.CAP_DELETE);
		capability.setCapability(AdapterCapability.CAP_DIST_AGGREGATES);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_GROUPBY);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_GROUPBY);
	
		capability.setCapability(AdapterCapability.CAP_IN);
		capability.setCapability(AdapterCapability.CAP_JOINS);
		capability.setCapability(AdapterCapability.CAP_JOINS_FULL_OUTER);
		capability.setCapability(AdapterCapability.CAP_JOINS_OUTER);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_FULL_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_GROUPBY);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_INNER_JOIN);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_LEFT_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capability.setCapability(AdapterCapability.CAP_OFFSET);
		
		capability.setCapability(AdapterCapability.CAP_OR);
		capability.setCapability(AdapterCapability.CAP_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_ORDERBY_EXPRESSIONS);
	
		capability.setCapability(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);
	
		capability.setCapability(AdapterCapability.CAP_PROJECT);
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_SEQUENCE_EXPRESSION);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_FULL_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_INNER_JOIN);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_LEFT_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_SUBQUERY);
		capability.setCapability(AdapterCapability.CAP_SUBQUERY_UPDATE);
		
		capability.setCapability(AdapterCapability.CAP_TSQL_DELUPD);
	
		capability.setCapability(AdapterCapability.CAP_UPDATE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_OWNER_SUPPORTED);
	
		capability.setCapability(AdapterCapability.CAP_DISTINCT);
		capability.setCapability(AdapterCapability.CAP_HAVING);
		capability.setCapability(AdapterCapability.CAP_BI_POWER);
		capability.setCapability(AdapterCapability.CAP_BI_ROUND);
		capability.setCapability(AdapterCapability.CAP_BI_SIGN);
		capability.setCapability(AdapterCapability.CAP_BI_SIN);
		capability.setCapability(AdapterCapability.CAP_BI_SQRT);
		capability.setCapability(AdapterCapability.CAP_BI_TAN);
		capability.setCapability(AdapterCapability.CAP_BI_LTRIM);
		capability.setCapability(AdapterCapability.CAP_BI_RTRIM);
		capability.setCapability(AdapterCapability.CAP_BI_ABS);
		capability.setCapability(AdapterCapability.CAP_BI_COS);
		capability.setCapability(AdapterCapability.CAP_BI_ROWID);
		capability.setCapability(AdapterCapability.CAP_BI_EXP);
		capability.setCapability(AdapterCapability.CAP_BI_FLOOR);
		capability.setCapability(AdapterCapability.CAP_BI_LENGTH);
		capability.setCapability(AdapterCapability.CAP_LIKE);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
	
		*/
		
		//enabled capabilities
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_AND);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_IN);
		capability.setCapability(AdapterCapability.CAP_OR);
		capability.setCapability(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_PROJECT);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_DIST_AGGREGATES);
		capability.setCapability(AdapterCapability.CAP_BETWEEN);
		capability.setCapability(AdapterCapability.CAP_DISTINCT);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
		capability.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capability.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		capability.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capability.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_LIKE);
		capability.setCapability(AdapterCapability.CAP_HAVING);
		capability.setCapability(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capability.setCapability(AdapterCapability.CAP_AGGREGATES);
		capability.setCapability(AdapterCapability.CAP_ORDERBY);
		capability.setCapability(AdapterCapability.CAP_GROUPBY);
		capability.setCapability(AdapterCapability.CAP_JOINS);
		capability.setCapability(AdapterCapability.CAP_JOINS_FULL_OUTER);
		capability.setCapability(AdapterCapability.CAP_JOINS_OUTER);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
		capability.setCapability(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
		capability.setCapability(AdapterCapability.CAP_DELETE);
		capability.setCapability(AdapterCapability.CAP_UPDATE);
		capability.setCapability(AdapterCapability.CAP_INSERT);
		capability.setCapability(AdapterCapability.CAP_INSERT_SELECT);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);
		capability.setCapability(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);
		
		
		return capability;
		
		
		/* from proeprty.bq.ini for SDA
		 * 
		 	CAP_CRT_TEMP_TABLES : false
		 	CAP_GROUPING_SETS : false
			CAP_INSERT : false
			CAP_OFFSET_ARG : false
			CAP_ORDERBY_NULLS_ORDERING : false
			CAP_PARAM_FUNCTION_SUBSTITUTION : false
			CAP_TABLE_CAP : false
			CAP_TOP_UNDER_UNION : false
			CAP_TRUNCATE_TABLE : false
		 	CAP_UNIONALL : false
		 	
		 	
		    CAP_AGGREGATES : true
			CAP_AGGREGATES_COLNAME : true
			CAP_AGGR_STDDEV : true
			CAP_AND : true
			CAP_AND_DIFFERENT_COLUMNS : true
			CAP_ARRAY_FETCH : true
			CAP_BETWEEN : true
			CAP_CASE_EXPRESSION : true
			CAP_COLUMN_CAP : true
			CAP_CORRELATION_IN_UPDATE : true
			CAP_DELETE : true
			CAP_DIST_AGGREGATES : true
			CAP_EXPR_IN_FULL_OUTER_JOIN : true
			CAP_EXPR_IN_GROUPBY : true
			CAP_EXPR_IN_INNER_JOIN : true
			CAP_EXPR_IN_LEFT_OUTER_JOIN : true
			CAP_EXPR_IN_ORDERBY : true
			CAP_EXPR_IN_PROJ : true
			CAP_EXPR_IN_WHERE : true
			CAP_GROUPBY : true
			CAP_IN : true
			CAP_JOINS : true
			CAP_JOINS_FULL_OUTER : true
			CAP_JOINS_OUTER : true
			CAP_LIKE : true
			CAP_LIMIT : true
			CAP_NESTED_FUNC_IN_FULL_OUTER_JOIN : true
			CAP_NESTED_FUNC_IN_GROUPBY : true
			CAP_NESTED_FUNC_IN_INNER_JOIN : true
			CAP_NESTED_FUNC_IN_LEFT_OUTER_JOIN : true
			CAP_NESTED_FUNC_IN_ORDERBY : true
			CAP_NESTED_FUNC_IN_PROJ : true
			CAP_NESTED_FUNC_IN_WHERE : true
			CAP_NONEQUAL_COMPARISON : true
			CAP_OFFSET : true
			CAP_OR : true
			CAP_ORDERBY : true
			CAP_ORDERBY_EXPRESSIONS : true
			CAP_OR_DIFFERENT_COLUMNS : true
			CAP_PROJECT : true
			CAP_SELECT : true
			CAP_SEQUENCE_EXPRESSION : true
			CAP_SIMPLE_EXPR_IN_FULL_OUTER_JOIN : true
			CAP_SIMPLE_EXPR_IN_GROUPBY : true
			CAP_SIMPLE_EXPR_IN_INNER_JOIN : true
			CAP_SIMPLE_EXPR_IN_LEFT_OUTER_JOIN : true
			CAP_SIMPLE_EXPR_IN_ORDERBY : true
			CAP_SIMPLE_EXPR_IN_PROJ : true
			CAP_SIMPLE_EXPR_IN_WHERE : true
			CAP_SUBQUERY : true
			CAP_SUBQUERY_UPDATE : true
			CAP_TOP : true
			CAP_TSQL_DELUPD : true
			CAP_UPDATE : true
			CAP_WHERE : true
			CAP_OWNER_SUPPORTED : true
			CAP_SUBQUERY_DELETE : true
			CAP_DISTINCT : true
			CAP_HAVING : true
			CAP_BI_POWER : true
			CAP_BI_ROUND : true
			CAP_BI_SIGN : true
			CAP_BI_SIN : true
			CAP_BI_SQRT : true
			CAP_BI_TAN : true
			CAP_BI_LTRIM : true
			CAP_BI_RTRIM : true
			CAP_BI_ABS : true
			CAP_BI_COS : true
			CAP_BI_ROWID : true
			CAP_BI_EXP : true
			CAP_BI_FLOOR : true
			CAP_BI_LENGTH : true
		 */
	}

	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
		if (lobindex.contains(lobId) == false) {
			return 0;
		} else {
			byte[] data = lobindex.get(lobId);
			Integer startpoint = lobindexlastread.get(lobId);
			if (startpoint == null) {
				startpoint = 0;
			}
			int length = bufferSize;
			if (data.length-startpoint<length) {
				length = data.length-startpoint;
				System.arraycopy(data, startpoint, bytes, 0, length);
				lobindex.remove(lobId);
				lobindexlastread.remove(lobId);
			} else {
				System.arraycopy(data, startpoint, bytes, 0, length);
				lobindexlastread.put(lobId, length);
			}
			return length;
		}
	}

	
	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		
		RemoteSourceDescription rs = new RemoteSourceDescription();

		PropertyGroup connectionInfo = new PropertyGroup("connParam", "Connection Parameters", "The Connection parameters for the connection");
		
		//prompt in the UI
		PropertyEntry p = new PropertyEntry("catalog_id", "Data Catalog", "Bigquery Data Catalog ID", true);
		connectionInfo.addProperty(p);
		
		p = new PropertyEntry("dataset_id", "Dataset ID", "Bigquery Dataset ID", true);
		connectionInfo.addProperty(p);
		
		
		p = new PropertyEntry("help", "Help", "help", true);
		p.setDefaultValue("Make sure to enter the Project and Authentication info in /usr/sap/dataprovagent/adapters/bqadapter.properties file");
		connectionInfo.addProperty(p);
	
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		// BigQuery version
		return "Google BigQuery";
	}

	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		logger.debug("### importMetadata for table: "+nodeId);
		List<Column> schema = new ArrayList<Column>();
		try {
			String query = "SELECT COLUMN_NAME AS COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM `"+BQAdapter.catalogID+"`."+BQAdapter.dataSetID+".INFORMATION_SCHEMA.COLUMNS where TABLE_NAME ='"+nodeId+"'";
			logger.debug("Query: " + query);
			PreparedStatement st = conn.prepareStatement(query);					
		
			ResultSet rs = st.executeQuery();
			while (rs.next()) {				
				int length = 0;
				Column col = getHANAColumn(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"), rs.getString("DATA_TYPE"),
						rs.getString("IS_NULLABLE") == "YES", 0, 0,length);
				schema.add(col);
			}
		} catch (SQLException e) {
			logger.warn( "Error reading table metadata");
			logger.error( e.getMessage(), e);
			throw new AdapterException(e, e.getMessage());
		}
				
		TableMetadata table = new TableMetadata();
		table.setName(nodeId);
		table.setColumns(schema);
		logger.debug("ImportMetadata: setting this.data: ");
		BQAdapter.data = new AdapterRowSet(table.getColumns());
		return table;
	}
	
	
	
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException  {
			
		logger.info("### BQAdapter open");
		
		// Reading properties from .properties file
		String os = System.getProperty("os.name");
		logger.debug("os: "+os);
				
		if (os.toLowerCase().contains("windows"))
			BQAdapter.propFile = "C:/usr/sap/dataprovagent/adapters/bqadapter.properties";
		else
			BQAdapter.propFile = "/usr/sap/dataprovagent/adapters/bqadapter.properties";
		logger.debug("BQAdapter.propFile: "+BQAdapter.propFile);
				
		FileReader reader;
	    Properties pfile = new Properties();
		try {
			reader = new FileReader(BQAdapter.propFile);
			pfile.load(reader);
			} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
				
		BQAdapter.projectID = pfile.getProperty("Project");
		BQAdapter.OAuth_email = pfile.getProperty("Email");
		BQAdapter.OAuth_key_path = pfile.getProperty("KeyFilePath");
		BQAdapter.jdbcjar = pfile.getProperty("Driver");
		
		BQAdapter.dataSetID = connectionInfo.getConnectionProperties().getPropertyEntry("dataset_id").getValue();
		BQAdapter.catalogID = connectionInfo.getConnectionProperties().getPropertyEntry("catalog_id").getValue();

		logger.debug(BQAdapter.projectID+":"+BQAdapter.OAuth_email+":"+BQAdapter.OAuth_key_path+":"+BQAdapter.jdbcjar+":"+BQAdapter.dataSetID+":"+BQAdapter.catalogID);
		
		if (!BQAdapter.driverLoaded){
	    try 
		    {	    		    	
	    		File libs = new File(BQAdapter.jdbcjar);
			    File[] jars = libs.listFiles(new FileFilter() {
			        public boolean accept(File pathname) {
			            return pathname.getName().toLowerCase().endsWith(".jar");
			        }
			    });
	
			    URL[] urls = new URL[jars.length];
			    for (int i=0; i<jars.length; i++) {
			    	//logger.info("### Adding JAR : "+jars[i]);		    	
			        urls[i] = jars[i].toURI().toURL();
			    }
			    ClassLoader uc = new URLClassLoader(urls,this.getClass().getClassLoader());
				
				Driver d;
				try {
					d = (Driver)Class.forName("com.simba.googlebigquery.jdbc41.Driver", true, uc).newInstance();
				} catch (ClassNotFoundException e) {
					logger.debug("### Error loading Simba jars : "+e);
					logger.error(e.getMessage(), e);
					throw new AdapterException(e,
							"com.simba.googlebigquery.jdbc41.Driver not found in the jar file.");
				}
				DriverManager.registerDriver(new DriverDelegator(d));
				BQAdapter.driverLoaded = true;
		    } 
		 catch (Exception e) 
		    {
			 logger.error(e.getMessage(), e);
			 throw new AdapterException(e);
		    } 
		}
		
		String connStr = "";
	    try 
		{
	    	if(conn == null){
	    		String perfSetting = ";AllowLargeResults=1;EnableHighThroughputAPI=1;HighThroughputMinTableSize=10000;Timeout=10000;";
		    	connStr = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId="+BQAdapter.projectID+perfSetting+"OAuthType=0;OAuthServiceAcctEmail="+BQAdapter.OAuth_email+";OAuthPvtKeyPath="+BQAdapter.OAuth_key_path;
				conn = DriverManager.getConnection(connStr);
				logger.debug("### BQAdapter New Connection Success "+connStr);
			}
		} 
		catch (SQLException e) 
		{
			logger.debug("### BQAdapter Error Connecting to "+connStr);			
			logger.error(e.getMessage(), e);
			throw new AdapterException(e);
		}
	}
	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		//logger.info("### BQAdapter getNext");
		/**
		 * Currently this function gets called until it returns a empty row set.
		 */
		int resultRowsRead = 0;
		try {
			if (this.results.isAfterLast()) {
				this.results.close();
			} else {
				ResultSetMetaData metaData = this.results.getMetaData();
				int count = metaData.getColumnCount();
	
				while (resultRowsRead < this.fetchsize && this.results.next()) {
	
					int mappedIndex = 0;
					AdapterRow row = rows.newRow();
					resultRowsRead++;
					String typename;
					String classname;
	
					for (int colIndex = 1; colIndex <= count; colIndex++) {
						mappedIndex = colIndex - 1;
	
						Object value = this.results.getObject(colIndex);
						classname = metaData.getColumnClassName(colIndex);
						typename = metaData.getColumnTypeName(colIndex);		
						logger.debug("### Colclass:ColType: "+classname +":"+typename );
						if (value == null) {
							row.setColumnNull(mappedIndex);
						} else {
							switch (classname) {
							case "java.lang.Boolean":
								row.setColumnValue(mappedIndex, (((Boolean) value).booleanValue()==true?"1":"0"));
								break;
							case "[B":
								row.setColumnValue(mappedIndex, (byte[]) value);
								break;
							case "java.lang.Integer":
								row.setColumnValue(mappedIndex, (Integer) value);
								break;
							case "java.math.BigInteger":
								java.math.BigInteger bigintvalue = (java.math.BigInteger) value;
								row.setColumnValue(mappedIndex, bigintvalue.doubleValue());
								break;
							case "java.lang.Long":
								row.setColumnValue(mappedIndex, (Long) value);
								break;
							case "java.lang.Float":
								row.setColumnValue(mappedIndex, (Float) value);
								break;
							case "java.lang.Double":
								row.setColumnValue(mappedIndex, (Double) value);
								break;
							case "java.math.BigDecimal":
								row.setColumnValue(mappedIndex, (BigDecimal) value);
								break;
							case "java.sql.Date":
								if (typename.equals("YEAR")) {
									// A year datatype might be returned as java.sql.Date but its Hana datatype is INTEGER always - see importMetadata
									Calendar cal = Calendar.getInstance();
									cal.setTime((Date) value);
									row.setColumnValue(mappedIndex, cal.get(Calendar.YEAR));
								} else {
									row.setColumnValue(mappedIndex, new Timestamp(this.results.getDate(colIndex)));
								}
								break;
							case "java.sql.Timestamp":
								row.setColumnValue(mappedIndex, new Timestamp(this.results.getTimestamp(colIndex)));
								break;
							case "java.sql.Time":
								row.setColumnValue(mappedIndex, new Timestamp(this.results.getTime(colIndex)));
								break;
							case "java.sql.Short":
								row.setColumnValue(mappedIndex, ((Short) value).intValue());
								break;
							case "java.lang.String":
								switch (typename) {
								case "CHAR":
								case "VARCHAR":
								case "NVARCHAR":
								case "STRING":
									row.setColumnValue(mappedIndex, (String) value);
									break;
								case "TEXT":
								case "MEDIUMTEXT":
								case "LONGTEXT":
									String valuestring = (String) value;
									if (valuestring.length() < AdapterRow.MAX_CLOB_INLINE_LOB_LENGTH) {
										row.setColumnValue(mappedIndex, (String) value);
									} else {
										Long id = Long.valueOf(valuestring.hashCode());
										try {
											lobindex.put(id, valuestring.getBytes("UTF-8"));
											row.setColumnLobIdValue(mappedIndex, id, LobCharset.UTF_8);
										} catch (UnsupportedEncodingException e) {
										}
									}
									break;
								}
								break;
							default:
								logger.info("getNext: " + classname + " not handled");
							}
						}
					}
				}
			}

		} catch (SQLException e) {
			throw new AdapterException(e);
		}

	}

	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		logger.debug("### BQAdapter putNext, getRowCount: "+rows.getRowCount());
		
		int rowsNum = rows.getRowCount();
        int colsNum = rows.getColumns().size();
        int affectedRows = 0;
        try {
			for (int rowIndex = 0; rowIndex < rowsNum; rowIndex++) {
                AdapterRow row = rows.getRow(rowIndex);
                for (int colIndex = 0; colIndex < colsNum; colIndex++) {
                    int paramIndex = colIndex + 1; // 1-based
                    boolean isNull = row.isColumnNull(colIndex);
                    DataType dataType = row.getColumnDataType(colIndex);
                    switch (dataType) {
                  case BOOLEAN:
                        if (isNull)
                            pstmt.setNull(paramIndex, Types.BOOLEAN);
                        else
                        	pstmt.setBoolean(paramIndex, row.getColumnBooleanValue(colIndex));
                        break;
                       
                    case TINYINT:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.TINYINT);
                        else
                        	pstmt.setInt(paramIndex, row.getColumnIntValue(colIndex));
                        break;
                        
                    case BIGINT:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.BIGINT);
                        else
                        	pstmt.setLong(paramIndex, row.getColumnLongValue(colIndex));
                        break;
                        
                    case DOUBLE:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.DOUBLE);
                        else
                        	pstmt.setDouble(paramIndex, row.getColumnDoubleValue(colIndex));
                        break;
                        
                    case DECIMAL:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.DECIMAL);
                        else
                        	pstmt.setBigDecimal(paramIndex, row.getColumnDecimalValue(colIndex));
                        break;
                    case VARBINARY:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.VARBINARY);
                        else
                        	pstmt.setBytes(paramIndex, row.getColumnBytesValue(colIndex));
                        break;
                    case DATE:
                    case SECONDDATE:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.DATE);
                        else
                        	pstmt.setDate(paramIndex, new Date(
                                row.getColumnTimeValue(colIndex).getCalendarValue().getTimeInMillis()));
                        break;
                    case TIME:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.TIME);
                        else
                        	pstmt.setTime(paramIndex, new Time(
                                row.getColumnTimeValue(colIndex).getCalendarValue().getTimeInMillis()));
                        break;
                    case TIMESTAMP:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.TIMESTAMP);
                        else {
                            Timestamp ts = row.getColumnTimeValue(colIndex);   // SDK timestamp
                            java.sql.Timestamp dataTimestamp = new java.sql.Timestamp(
                                    ts.getCalendarValue().getTimeInMillis());
                            dataTimestamp.setNanos(ts.getSubSecond() * 100);
                            pstmt.setTimestamp(paramIndex, dataTimestamp);
                        }
                        break;
                    case VARCHAR:
                    case ALPHANUM:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.VARCHAR);
                        else
                        	pstmt.setString(paramIndex, row.getColumnStringValue(colIndex));
                        break;
                    case NVARCHAR:
                        if (isNull)
                        	pstmt.setNull(paramIndex, Types.NVARCHAR);
                        else
                        	pstmt.setString(paramIndex, row.getColumnStringValue(colIndex));
                        break;
                    default:
                        throw new AdapterException("Unexpected data type " + dataType + ".");
                    } //switch
                }  //for 
                pstmt.executeUpdate();
                ++affectedRows;
			} //end for

			logger.debug("### BQAdapter putNext, affRows: "+affectedRows);
		} //end try
        catch (SQLException e) {
			logger.error(e.getMessage(), e);
        }
        return affectedRows;
    }
			        

	@Override
	public void rollbackTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		close();
	}

	/*
	 * Set the browse id for anode to expnd. eg: Expand the tables for a
	 * database node
	 */
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
		this.browseNodeId = nodeId;

	}

	/*
	 * Set the fetch size. This setting is provided from dpagentconfig.ini
	 */
	@Override
	public void setFetchSize(int fetchSize) {
		this.fetchsize = fetchSize;

	}

	@Override
	public String addSubscription(SubscriptionSpecification arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void beginMarker(String arg0, SubscriptionSpecification arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void committedChange(SubscriptionSpecification arg0) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void endMarker(String arg0, SubscriptionSpecification arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public Capabilities<AdapterCapability> getCDCCapabilities(String version) throws AdapterException {

		return this.getCapabilities(version);
	}

	@Override
	public void removeSubscription(SubscriptionSpecification arg0) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean requireDurableMessaging() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start(ReceiverConnection arg0, SubscriptionSpecification arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop(SubscriptionSpecification arg0) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean supportsRecovery() {
		return false;
	}
	
  private Column getHANAColumn(String columnName, String dataType, String colType, boolean nullable, int precision, int scale, int length) {
	  //	logger.info("### getHANAColumn : "+columnname);
		Column col = new Column();
		col.setName(columnName);
		col.setNullable(nullable);
//		logger.info("query type:" + columnName + "," + dataType + ", " + colType + ", " + length);
		if (precision != 0) {
			col.setPrecision(precision);
		}
		if (scale != 0) {
			col.setScale(scale);
		}
		col.setLength(length);

/*Snowflake to Hana type mapping
 *  SF										Hana
	int64  8 bytes           				bigint
	numeric  16 byts         				decimal
	float64 8 byts          				double
	bool   true/false        				tinyint
	string var length unicode 				nvarchar
	bytes var length binary    				varbinary
	date calendar date          			date
	datetime - datetime         			seconddate
	time - hr:mn:sec:microsec   			time
	timestamp - microsec precision     		timestamp
	array   ordered list				    NOT MAPPED
	geography  points, lines. polygons      NOT MAPPed
	RECORD/STRUCT							not mapped
*/
		
	
		switch (dataType) {
		case "BOOLEAN":
		case "BOOL":
			col.setDataType(DataType.TINYINT);
			break;
		
		case "INT64":
		case "INTEGER":
			col.setDataType(DataType.BIGINT);
			break;
			
		// BigQuery Numeric Type Overview (decimals)
		case "FLOAT64":
		case "FLOAT":
			col.setDataType(DataType.DOUBLE);
			break;
		case "NUMERIC":
			col.setDataType(DataType.DECIMAL);
			break;
			
		// BigQuery date/time datatype
		case "datetime":
		case "DATETIME":
		case "timestamp":
		case "TIMESTAMP":
			col.setDataType(DataType.TIMESTAMP);
			break;
			
		case "date":
		case "DATE":
			col.setDataType(DataType.DATE);
			break;
			
		case "time":
		case "TIME":
			col.setDataType(DataType.TIME);
			break;
		
			
		// BigQuery char datatypes
		case "STRING":
			col.setDataType(DataType.NVARCHAR);
			break;
			
		// BigQuery binary datatypes
		case "BYTES":
			col.setDataType(DataType.VARBINARY);
			break;

		default:  // all umapped datatyps like STRUCT, ARRAY, geography is mapped to varchar
			col.setDataType(DataType.VARCHAR);
			break;
		}
		return col;
	}


	@Override
	public AdapterStatistics getAdapterStatistics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAdapterStatisticsUpdateInterval(int arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void startLatencyTicket(LatencyTicketSpecification arg0) throws AdapterException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void executeStatement(String sql, StatementInfo info) throws AdapterException {

		try {
			sql = sql.replace("\"", "");
			
			logger.debug("### Executing original : "+sql);
			String cusQuery = SQLRewriter.rewriteSQL(sql);
	
			logger.debug("### Enhanced Query : "+cusQuery);
			
			PreparedStatement st = conn.prepareStatement(cusQuery);
			this.results = st.executeQuery();
		} catch (SQLException e) {
			logger.warn(e.getMessage(), e);
		}
		
		
		//old version
		/*try {
				sql = sql.replace("\"", "");
				
				logger.info("### Executing original : "+sql);
							
				String tablePrefix = "`"+this.projectID+"`."+this.dataSetID+".";
				String[] parts = sql.split(" FROM ");
				String cusQuery = parts[0]+" FROM "+tablePrefix+parts[1];
				logger.info("### Enhanced Query : "+cusQuery);
				
				PreparedStatement st = conn.prepareStatement(cusQuery);
				this.results = st.executeQuery();
			} catch (SQLException e) {
				logger.log(Level.WARN, e.getMessage(), e);
			}
			*/
	}

	@Override
	public void executePreparedInsert(String sql, StatementInfo arg1) throws AdapterException {
		try {
			sql = sql.replace("\"", "");
			String tablePrefix = "`"+this.projectID+"`."+this.dataSetID+".";
			String[] parts = sql.split(" INTO ");
			
			String cusQuery = parts[0]+" INTO "+tablePrefix+parts[1];
			logger.debug("### BQAdapter executePreparedInsert : "+cusQuery);
			this.pstmt = conn.prepareStatement(cusQuery);
		} catch (SQLException e) {
			logger.warn(e.getMessage(), e);
		}
		
	}

	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter executePreparedUpdate : "+arg0);

	}
	
	@Override
    public int executeUpdate(String sql, StatementInfo info)
            throws AdapterException {       
        String newSql = sql.replace("\"", "");; // rewriteResult.getNewSql();
        String firstWord = newSql.substring(0, newSql.indexOf(" "));
        String tablePrefix = "`"+this.projectID+"`."+this.dataSetID+".";
       
        if (firstWord.equalsIgnoreCase("update")) {
        	String[] parts = newSql.split(firstWord);
    		newSql = "UPDATE "+tablePrefix+parts[1].trim();
    		logger.debug("### Enhanced update Query : "+newSql);
        	
        }
        else if (firstWord.equalsIgnoreCase("delete")){
        	String[] parts = newSql.split(" FROM ");
    		newSql = parts[0]+" FROM "+tablePrefix+parts[1];
    		logger.debug("### Enhanced delete Query : "+newSql);
        }
        int affectedRows = 0;
        try {
           this.pstmt = conn.prepareStatement(newSql);
           affectedRows = this.pstmt.executeUpdate();
        } catch (SQLException e) {
            String message = "Failed to execute query [" + newSql + "].";
            logger.error(message, e);
            throw new AdapterException(e, message + "Error: " + e.getMessage());
        }
        return affectedRows;
    }

	
	@Override
	public Metadata importMetadata(String arg0, List<Parameter> arg1) throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter importMetadata : "+arg0);
		return null;
	}

	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter loadColumnsDictionary");
		return null;
	}

	@Override
	public List<BrowseNode> loadTableDictionary(String arg0) throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter loadColumnsDictionary : "+arg0);
		return null;
	}

	@Override
	public ParametersResponse queryParameters(String arg0, List<Parameter> arg1) throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter queryParameters : "+arg0);
		return null;
	}

	@Override
	public void setAutoCommit(boolean arg0) throws AdapterException {
		// TODO Auto-generated method stub
		logger.debug("### BQAdapter setAutoCommit : "+arg0);
	}

	@Override
	public void closeResultSet() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void executeCall(FunctionMetadata arg0) throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Metadata getMetadataDetail(String arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableProcedure prepareCall(ProcedureMetadata arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNodesListFilter(RemoteObjectsFilter arg0) throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void validateCall(FunctionMetadata arg0) throws AdapterException {
		// TODO Auto-generated method stub
		
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
	
	FilenameFilter bigqueryjarFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.endsWith(".jar") && name.startsWith("GoogleBigQueryJDBC41")) {
	            return true;
	        } else {
	            return false;
	        }
	    }
	};
	
}