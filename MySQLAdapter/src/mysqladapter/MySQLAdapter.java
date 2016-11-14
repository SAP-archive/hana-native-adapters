package mysqladapter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
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

import org.apache.log4j.Level;
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
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
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
 * SAP Jam Mysql Adapter.
 */
public class MySQLAdapter extends AdapterCDC {

	static Logger logger = LogManager.getLogger(MySQLAdapter.class.getName());
	private Connection conn = null;
	private String databaseName;
	private int fetchsize;
	private String browseNodeId;
	private int metaOffset;
	private ResultSet results;
	
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
			if (browseNodeId == null) {
				isImportable = true;
				rs = getTablesForDatabase(this.databaseName);

				while (rs.next()) {
					String nodeName = rs.getString(1);
					BrowseNode node = new BrowseNode(nodeName, nodeName );
					node.setImportable(isImportable);
					node.setExpandable(isExpandable);
					nodes.add(node);
				}

			} else {
				logger.log(Level.WARN, "Provided browseNodeId. Don't know how this happened");
			}
		} catch (SQLException e) {
			throw new AdapterException(e);
		}
		return nodes;
	}

	private ResultSet getTablesForDatabase(String database) throws SQLException {
		PreparedStatement st = this.conn.prepareStatement(
				"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? LIMIT ? OFFSET ?");
		st.setString(1, databaseName);
		st.setInt(2, this.fetchsize);
		st.setInt(3, this.metaOffset);
		ResultSet rs = st.executeQuery();
		return rs;
	}

	@Override
	public void close() throws AdapterException {
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

			logger.log(Level.INFO, "Closed the connection");
		} catch (SQLException e) {
			throw new AdapterException(e);
		}
	}

	@Override
	public void commitTransaction() throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void executeStatement(String sql, StatementInfo info) throws AdapterException {
		try {
			// logger.log(Level.INFO, sql);
			sql = sql.replace("\"", "");
			PreparedStatement st = conn.prepareStatement(sql);
			logger.log(Level.INFO, sql);
			this.results = st.executeQuery();
		} catch (SQLException e) {
			logger.log(Level.WARN, e.getMessage(), e);
		}
	}

	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capability = new Capabilities<AdapterCapability>();
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_AND);
		capability.setCapability(AdapterCapability.CAP_PROJECT);
		capability.setCapability(AdapterCapability.CAP_JOINS);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
		capability.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capability.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		capability.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capability.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_LIKE);
		capability.setCapability(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capability.setCapability(AdapterCapability.CAP_AGGREGATES);
		return capability;
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
	public void getNext(AdapterRowSet rows) throws AdapterException {
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
						if (value == null) {
							row.setColumnNull(mappedIndex);
						} else {
							classname = metaData.getColumnClassName(colIndex);
							typename = metaData.getColumnTypeName(colIndex);
	
							switch (classname) {
							case "java.lang.Boolean":
								switch (typename) {
								case "BIT": // BIT is always a VARCHAR in Hana
									row.setColumnValue(mappedIndex, (((Boolean) value).booleanValue()==true?"1":"0"));
									break;
								case "TINYINT": // TINYINT[1], BOOL, BOOLEAN might be a java.langBoolean and a INTEGER in Hana
									row.setColumnValue(mappedIndex, (((Boolean) value).booleanValue()==true?1:0));
									break;
								}
								break;
							case "[B":
								switch (typename) {
								case "BIT": // A BIT is a VARCHAR in Hana
									StringBuffer buffer = new StringBuffer();
									byte[] bytes = (byte[]) value;
									for(byte b : bytes) {
										buffer.append(Integer.toBinaryString(b));
									}
									row.setColumnValue(mappedIndex, buffer.toString());
									break;
								case "TINYBLOB":
									row.setColumnValue(mappedIndex, ((byte[]) value));
									break;
								case "LONGBLOB":
								case "MEDIUMBLOB":
								case "BLOB":
									byte[] valuebytes = (byte[]) value;
									if (valuebytes.length < AdapterRow.MAX_ASCII_INLINE_LOB_LENGTH) {
										row.setColumnValue(mappedIndex, valuebytes);
									} else {
										lobindex.put(Long.valueOf(valuebytes.hashCode()), valuebytes);
									}
									break;
								}
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
								case "TINYTEXT":
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
								logger.info("Class: " + classname + " not handled");
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
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();

		PropertyGroup connectionInfo = new PropertyGroup("connParam", "Connection Parameters",
				"The Connection parameters for the connection");
		PropertyEntry p = new PropertyEntry("server", "Server", "The address of the Mysql server", true);
		// p.setDefaultValue("mo-1c100dee5.mo.sap.corp");
		connectionInfo.addProperty(p);
		p = new PropertyEntry("port", "Port", "The port number of the Mysql server", true);
		p.setDefaultValue("3306");
		connectionInfo.addProperty(p);

		p = new PropertyEntry("databaseName", "Database name", "The name of the database to connect to", true);
		p.setDefaultValue("sys");
		connectionInfo.addProperty(p);

		p = new PropertyEntry("jdbcjar", "Location of mysql jdbc jar file", "The full path to the mysql jdbc driver jar file on the agent host", true);
		String libdirpath = System.getProperty("user.dir") + System.getProperty("file.separator") + "lib";
		File libdir = new File(libdirpath);
		if (libdir.exists() && libdir.isDirectory()) {
			File[] jars = libdir.listFiles(mysqljarfilesFilter);
			if (jars != null && jars.length > 0) {
				try {
					p.setDefaultValue(jars[0].getCanonicalPath());
				} catch (IOException e) {
					logger.log(Level.INFO, "Cannot convert File into canonical path");
				}
			}
		}
		connectionInfo.addProperty(p);

		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "Mysql Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		// Mysql version
		return "5.6.19";
	}

	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		List<Column> schema = new ArrayList<Column>();
		try {
			PreparedStatement st = conn.prepareStatement(
					"SELECT COLUMN_NAME AS COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATA_TYPE, COLUMN_TYPE, " +
					" COLUMN_DEFAULT, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
			st.setString(1, this.databaseName);
			st.setString(2, nodeId);
			// st.setInt(3, this.fetchsize);
			// st.setInt(4, this.metaOffset);
			logger.log(Level.INFO, "Query: " + st.toString());
			ResultSet rs = st.executeQuery();
			while (rs.next()) {
				String s = rs.getString("CHARACTER_MAXIMUM_LENGTH");
				int length = 0;
				if (s != null && !s.isEmpty()) {
					try {
						length = Integer.parseInt(s);
					} catch (NumberFormatException ex) {
						logger.log(Level.WARN, "Parsing int: " + s);
					}
				}
				Column col = getHANAColumn(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"), rs.getString("COLUMN_TYPE"),
						rs.getString("IS_NULLABLE") == "YES", rs.getInt("NUMERIC_PRECISION"), rs.getInt("NUMERIC_SCALE"),
						length);
				schema.add(col);
			}
		} catch (SQLException e) {
			logger.log(Level.WARN, "Error reading table metadata");
			logger.log(Level.ERROR, e.getMessage(), e);
			throw new AdapterException(e, e.getMessage());
		}
		TableMetadata table = new TableMetadata();
		table.setName(nodeId);
		table.setColumns(schema);
		return table;
	}

	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
		String jdbcjar = connectionInfo.getConnectionProperties().getPropertyEntry("jdbcjar").getValue();
		
		try {
			
			URL u = new URL("jar:file:" + jdbcjar + "!/");
			URLClassLoader ucl = new URLClassLoader(new URL[] { u });
			
			Driver d;
			try {
				d = (Driver)Class.forName("com.mysql.jdbc.Driver", true, ucl).newInstance();
			} catch (ClassNotFoundException e) {
				logger.log(Level.WARN, "Error loading com.mysql.jdbc.Driver");
				logger.log(Level.ERROR, e.getMessage(), e);
				throw new AdapterException(e,
						"com.mysql.jdbc.Driver not found in the jar file.");
			}
			DriverManager.registerDriver(new DriverDelegator(d));
		} catch (Exception e) {
			throw new AdapterException(e);
		} 


		String username = "";
		String password = "";
		try {
			username = new String(
					connectionInfo.getCredentialProperties().getCredentialEntry("credential").getUser().getValue(),
					"UTF-8");
			password = new String(
					connectionInfo.getCredentialProperties().getCredentialEntry("credential").getPassword().getValue(),
					"UTF-8");
		} catch (UnsupportedEncodingException ex) {
			logger.log(Level.WARN, "Error reading credentials");
			logger.log(Level.ERROR, ex.getMessage(), ex);
			throw new AdapterException(ex);
		}
		String server = connectionInfo.getConnectionProperties().getPropertyEntry("server").getValue();
		int port = Integer
				.parseUnsignedInt(connectionInfo.getConnectionProperties().getPropertyEntry("port").getValue());
		this.databaseName = connectionInfo.getConnectionProperties().getPropertyEntry("databaseName").getValue();

		try {
			conn = DriverManager.getConnection("jdbc:mysql://" + server + ":" + port + "/" + this.databaseName
					+ "?zeroDateTimeBehavior=convertToNull", username, password);
		} catch (SQLException e) {
			logger.log(Level.WARN, "Error opening connection");
			logger.log(Level.ERROR, e.getMessage(), e);
			throw new AdapterException(e);
		}
	}

	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		return 0;
		// TODO Auto-generated method stub

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
	
	private Column getHANAColumn(String columnname, String mysqltype, String mysqltypedetails, boolean nullable, int precision, int scale, int length) {
		
		Column col = new Column();
		col.setName(columnname);
		col.setNullable(nullable);
		System.out.println(mysqltype + ", " + mysqltypedetails + ", " + length);
		if (precision != 0) {
			col.setPrecision(precision);
		}
		if (scale != 0) {
			col.setScale(scale);
		}
		col.setLength(length);


		switch (mysqltype) {
		// MySQL Numeric Type Overview (integers)
		case "bit":
			col.setDataType(DataType.VARCHAR);
			if (length == 0) {
				if (precision == 0) {
					col.setLength(64);
				} else {
					col.setLength(precision);
				}
			}
			break;
		case "bigint":
			col.setDataType(DataType.BIGINT);
			break;
		case "int":
		case "integer":
			if (mysqltypedetails.contains("unsigned")) {
				col.setDataType(DataType.BIGINT);
			} else {
				col.setDataType(DataType.INTEGER);
			}
			break;
		case "tinyint":
		case "bool":
		case "boolean":
			col.setDataType(DataType.TINYINT);
			break;
		case "smallint":
		case "mediumint":
			col.setDataType(DataType.INTEGER);
			break;
			
		// MySQL Numeric Type Overview (decimals)
		case "double":
			col.setDataType(DataType.DOUBLE);
			break;
		case "float":
			col.setDataType(DataType.REAL);
			break;
		case "dec":
		case "decimal":
			if (mysqltypedetails.contains("unsigned")) {
				col.setDataType(DataType.DOUBLE);
			} else {
				col.setDataType(DataType.DECIMAL);
			}
			break;
			
		// MySQL date/time datatype
		case "datetime":
		case "timestamp":
			col.setDataType(DataType.TIMESTAMP);
			break;
		case "date":
			col.setDataType(DataType.DATE);
			break;
		case "time":
			col.setDataType(DataType.TIME);
			break;
		case "year":
			col.setDataType(DataType.INTEGER);
			break;
			
		// MySQL char datatypes
		case "varchar":
		case "char":
		case "tinytext":
			col.setDataType(DataType.NVARCHAR);
			break;
		case "longtext":
		case "text":
		case "mediumtext":
			col.setDataType(DataType.NCLOB);
			break;
			
		// MySQL binary datatypes
		case "binary":
			col.setDataType(DataType.VARBINARY);
			break;
		case "varbinary":
			col.setDataType(DataType.VARBINARY);
			break;
		case "tinyblob":
			col.setDataType(DataType.VARBINARY);
			break;
		case "longblob":
		case "mediumblob":
		case "blob":
			col.setDataType(DataType.BLOB);
			break;

		// other datatypes
		case "enum":
			col.setDataType(DataType.NVARCHAR);
			break;
		case "set":
			col.setDataType(DataType.NVARCHAR);
			break;
		default:
			col.setDataType(DataType.INVALID);
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
	public void executePreparedInsert(String arg0, StatementInfo arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public int executeUpdate(String arg0, StatementInfo arg1) throws AdapterException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Metadata importMetadata(String arg0, List<Parameter> arg1) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BrowseNode> loadTableDictionary(String arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ParametersResponse queryParameters(String arg0, List<Parameter> arg1) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAutoCommit(boolean arg0) throws AdapterException {
		// TODO Auto-generated method stub

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
	
	FilenameFilter mysqljarfilesFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.endsWith(".jar") && name.startsWith("mysql-connector-java")) {
	            return true;
	        } else {
	            return false;
	        }
	    }
	};
}