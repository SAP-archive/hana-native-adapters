package mysqladapter;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.LatencyTicketSpecification;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
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
	private int rowsReadTotal = 0;
	private int chunksReadTotal = 0;

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
					BrowseNode node = new BrowseNode(nodeName, nodeName.toUpperCase());
					node.setImportable(isImportable);
					node.setExpandable(isExpandable);
					nodes.add(node);
				}

			} else {
				logger.log(Level.WARN, "Provided browseNodeId. Don't know how this happened");
			}
		} catch (SQLException e) {
			logger.log(Level.WARN, "Error browsing metadata.");
			logger.log(Level.ERROR, e.getMessage(), e);
			throw new AdapterException(e, e.getMessage());
		}
		return nodes;
	}

	private ResultSet getTablesForDatabase(String database) throws SQLException {
		PreparedStatement st = this.conn.prepareStatement(
				"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA)=UPPER(?) LIMIT ? OFFSET ?");
		st.setString(1, databaseName);
		st.setInt(2, this.fetchsize);
		st.setInt(3, this.metaOffset);
		ResultSet rs = st.executeQuery();
		return rs;
	}

	@Override
	public void close() throws AdapterException {
		try {
			rowsReadTotal = 0;
			chunksReadTotal = 0;
			if (results != null) {
				results.close();
			}

			if (conn != null) {
				conn.close();
			}

			logger.log(Level.INFO, "Closed the connection");
		} catch (Exception e) {
			logger.log(Level.WARN, "Error closing the connection.");
			logger.log(Level.ERROR, e.getMessage(), e);
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
		capability.setCapability(AdapterCapability.CAP_JOINS);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
		capability.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capability.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
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
		return 0;
	}

	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		/**
		 * Currently this function gets called until it returns a empty row set.
		 */
		int resultRowsRead = 0;
		try {
			ResultSetMetaData metaData = this.results.getMetaData();
			int count = metaData.getColumnCount();

			while (resultRowsRead < this.fetchsize && this.results.next()) {

				int mappedIndex = 0;
				AdapterRow row = rows.newRow();
				resultRowsRead++;
				rowsReadTotal++;

				for (int colIndex = 1; colIndex <= count; colIndex++) {
					String mysqltype = metaData.getColumnTypeName(colIndex);
					mappedIndex = colIndex - 1;

					Object value = this.results.getObject(colIndex);
					if (value == null) {
						row.setColumnNull(mappedIndex);
						continue;
					}

					switch (mysqltype.toLowerCase()) {
					case "varchar":
					case "char":
						row.setColumnValue(mappedIndex, value.toString());
						break;
					case "bigint":
					case "bigint unsigned":
						row.setColumnValue(mappedIndex, this.results.getLong(colIndex));
						break;
					case "longtext":
						row.setColumnLobValue(mappedIndex, this.results.getBytes(colIndex), LobCharset.UTF_8);
						break;
					case "datetime":
						row.setColumnValue(mappedIndex, new Timestamp(value.toString()));
						break;
					case "int":
						int setVzal = this.results.getInt(colIndex);
						row.setColumnValue(mappedIndex, setVzal);
						break;
					case "int unsigned":
						row.setColumnValue(mappedIndex, this.results.getLong(colIndex));
						break;
					case "decimal":
					case "decimal unsigned":
						row.setColumnValue(mappedIndex, this.results.getDouble(colIndex));
						break;
					case "text":
						row.setColumnLobValue(mappedIndex, this.results.getBytes(colIndex), LobCharset.UTF_8);
						break;
					case "tinyint":
						row.setColumnValue(mappedIndex, this.results.getInt(colIndex));
						break;
					case "tinyblob":
						row.setColumnValue(mappedIndex, new String(this.results.getBytes(colIndex), "UTF-8"));
						break;
					case "date":
						row.setColumnValue(mappedIndex, new Timestamp(this.results.getDate(colIndex)));
						break;
					case "timestamp":
						row.setColumnValue(mappedIndex, new Timestamp(this.results.getTimestamp(colIndex)));
						break;
					case "float":
						row.setColumnValue(mappedIndex, this.results.getDouble(colIndex));
						break;
					case "longblob":
						row.setColumnLobValue(mappedIndex, this.results.getBytes(colIndex), LobCharset.UTF_8);
						break;
					case "mediumtext":
						row.setColumnLobValue(mappedIndex, this.results.getBytes(colIndex), LobCharset.UTF_8);
						break;
					case "mediumblob":
						row.setColumnLobValue(mappedIndex, this.results.getBytes(colIndex), LobCharset.UTF_8);
						break;
					case "enum":
						row.setColumnValue(mappedIndex, value.toString());
						break;
					}
				}
			}

			if (resultRowsRead == 0) {
				// all or no rows read
				logger.log(Level.INFO, "Rows read " + rowsReadTotal + " in " + chunksReadTotal + " chunks");
				rowsReadTotal = 0;
				chunksReadTotal = 0;

				logger.log(Level.INFO, "Trying to close the result set");
				if (null != this.results) {
					this.results.close();
					logger.log(Level.INFO, "Result set closed");
				}
			} else {
				chunksReadTotal++;
			}

		} catch (Exception e) {
			logger.log(Level.WARN, e.getMessage());
		}

	}

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();

		PropertyGroup connectionInfo = new PropertyGroup("connParam", "Connection Parameters",
				"The Connection parameters for the connection");
		PropertyEntry p = new PropertyEntry("server", "Server", "The address of the Mysql server", true);
		p.setDefaultValue("mo-1c100dee5.mo.sap.corp");
		connectionInfo.addProperty(p);
		p = new PropertyEntry("port", "Port", "The port number of the Mysql server", true);
		p.setDefaultValue("3306");
		connectionInfo.addProperty(p);

		p = new PropertyEntry("databaseName", "Database name", "The name of the database to connect to", true);
		p.setDefaultValue("ct_stage");
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
					"SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATA_TYPE, COLUMN_TYPE, COLUMN_DEFAULT, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?) ORDER BY ORDINAL_POSITION");
			st.setString(1, this.databaseName);
			st.setString(2, nodeId);
			// st.setInt(3, this.fetchsize);
			// st.setInt(4, this.metaOffset);
			logger.log(Level.INFO, "Query: " + st.toString());
			ResultSet rs = st.executeQuery();
			while (rs.next()) {
				String column_name = rs.getString("COLUMN_NAME");
				DataType dataType = getHANADatatype(rs.getString("DATA_TYPE"), rs.getString("COLUMN_TYPE"));
				Column col = new Column(column_name, dataType);
				col.setNullable(rs.getString("IS_NULLABLE") == "YES");
				if (rs.getInt("NUMERIC_PRECISION") != 0) {
					col.setPrecision(rs.getInt("NUMERIC_PRECISION"));
				}
				if (rs.getInt("NUMERIC_SCALE") != 0) {
					col.setScale(rs.getInt("NUMERIC_SCALE"));
				}
				int length = 0;
				String s = rs.getString("CHARACTER_MAXIMUM_LENGTH");
				if (s != null && !s.isEmpty()) {
					try {
						length = Integer.parseInt(s);
					} catch (NumberFormatException ex) {
						logger.log(Level.WARN, "Parsing int: " + s);
					}
					col.setLength(length);
				}
				if (dataType == DataType.NVARCHAR && length == 0) {
					col.setLength(255);
				}
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

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			logger.log(Level.WARN, "Error loading com.mysql.jdbc.Driver");
			logger.log(Level.ERROR, e.getMessage(), e);
			throw new AdapterException(e,
					"com.mysql.jdbc.Driver not found on the adapter class path. Please make sure that you have access to the driver.");
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
			throw new AdapterException(ex, ex.getMessage());
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
			throw new AdapterException(e, e.getMessage());
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

	private DataType getHANADatatype(String mysqltype, String mysqltypedetails) {
		switch (mysqltype) {
		case "varchar":
			return DataType.NVARCHAR;
		case "bigint":
			return DataType.BIGINT;
		case "longtext":
			return DataType.NCLOB;
		case "datetime":
		case "timestamp":
			return DataType.TIMESTAMP;
		case "int":
			if (mysqltypedetails.contains("unsigned")) {
				return DataType.BIGINT;
			} else {
				return DataType.INTEGER;
			}
		case "decimal":
			if (mysqltypedetails.contains("unsigned")) {
				return DataType.DOUBLE;
			} else {
				return DataType.DECIMAL;
			}
		case "text":
			return DataType.NCLOB;
		case "tinyint":
			return DataType.TINYINT;
		case "tinyblob":
			return DataType.NVARCHAR;
		case "date":
			return DataType.DATE;
		case "float":
			return DataType.DOUBLE;
		case "longblob":
			return DataType.BLOB;
		case "mediumtext":
			return DataType.NCLOB;
		case "mediumblob":
			return DataType.BLOB;
		case "enum":
			return DataType.NVARCHAR;
		default:
			return DataType.INVALID;
		}
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

}