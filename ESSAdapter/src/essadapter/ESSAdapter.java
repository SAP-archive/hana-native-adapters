package essadapter;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
*	Hello world Adapter.
*/
public class ESSAdapter extends AdapterCDC {

	
	private static final String HELLO = "HELLO";
	private static final String CREDENTIAL = "credential";
	private String username;
	private String name;
	private int fetchsize;
	private String currentbrowsenode;
	private String tablename_to_read;
	private ReceiverConnection receiverconnection;
	private HashMap<String, SubscriptionSpecification> activespecs = new HashMap<String, SubscriptionSpecification>();
	private HashMap<String, List<Column>> activespeccolumns = new HashMap<String, List<Column>>();
	private int rowsread;
	private Thread poller;
	
	/** Node user who is browsing **/
    protected String nodeID;
    
    /** Remote Objects Filter Object **/
	protected RemoteObjectsFilter remoteObjectsFilter = null;


	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		return "0.0.0";
	}

	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> caps = new Capabilities<AdapterCapability>();
		caps.setCapability(AdapterCapability.CAP_SELECT);
		caps.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		caps.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		return caps;
	}
	
	@Override
	public Capabilities<AdapterCapability> getCDCCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> caps = new Capabilities<AdapterCapability>();
		caps.setCapability(AdapterCapability.CAP_SELECT);
		caps.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		caps.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		return caps;
	}

	
	
	
	
	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		
		PropertyGroup connectionInfo = new PropertyGroup("conn","Connection Info","Connection Info");
		connectionInfo.addProperty(new PropertyEntry("name", "Hello whom?"));
		
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry(CREDENTIAL, "Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}

	
	
	
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
		
		@SuppressWarnings("unused")
		String password = "";
		try {
			username = new String(connectionInfo.getCredentialProperties().getCredentialEntry(CREDENTIAL).getUser().getValue(), "UTF-8");
			password = new String(connectionInfo.getCredentialProperties().getCredentialEntry(CREDENTIAL).getPassword().getValue(), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1);
		}
		
		name = connectionInfo.getConnectionProperties().getPropertyEntry("name").getValue();
	}
	
	@Override
	public void close() throws AdapterException {
		// There are no resources or connections to be closed
	}


	
	
	@Override
	public void setFetchSize(int fetchSize) {
		this.fetchsize = fetchSize;
	}


	
	
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
		this.currentbrowsenode = nodeId;
	}

	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		/**
		 * Generate a browse node tree for current remote source, or
		 * selected browse node entry.
		 */
		iList<BrowseNode> nodes = new ArrayList<BrowseNode>();
        if (nodeID == null) {
            try {
                while (resultSet.next()) {
                    BrowseNode node = new BrowseNode(schema, display);
                    node.setImportable(false);
                    node.setExpandable(true);
                    nodes.add(node);
                }
            } catch (SQLException e) {
                String message = "Failed to fetch schemas from ResultSet.";
                logger.error(message, e);
                throw new AdapterException(e, message);
            }
        } else {
            try {
                while(resultSet.next()) {
                    BrowseNode node = new BrowseNode(tableName);
                    node.setImportable(true);
                    node.setExpandable(false);
                    node.setNodeType(NodeType.TABLE);
                    node.setDescription(description);
                    nodes.add(node);
                }
            } catch (SQLException e) {
                String message = "Failed to fetch tables under schema " + nodeID + " from ResultSet.";
                logger.error(message, e);
                throw new AdapterException(e, message);
            }
        }
        return nodes;
	}
	
	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		/**
		 * This is the function to retreive metadata information in detail.
		 * Try to get these values from remote source:
		 * Table Level:
		 *     UNIQUE_NAME
		 *     PHYSICAL_NAME
		 *     REMOTE_OBJECT_TYPE
		 *     OWNER_NAME
		 *     DATABASE_NAME
		 *     DESCRIPTION
		 *     DEFAULT_LANGUAGE: table description's default language
		 *     IS_INSERTABLE
		 *     IS_UPDATEABLE
		 *     IS_DELETEABLE
		 *     IS_UPSERTABLE
		 *     IS_SELECTABLE
		 * 
		 * Column Level:
		 *     UNIQUE_NAME
		 *     COLUMN_NAME
		 *     COLUMN_POSITION
		 *     DATA_TYPE_NAME: converted hana data type 
		 *     LENGTH: converted hana data type length
		 *     NULLABLE
		 *     PRECISION : converted hana data type precision
		 *     SCALE: converted hana data type scale
		 *     REMOTE_DATA_TYPE_NAME: native data type
		 *     REMOTE_LENGTH: native data type length
		 *     REMOTE_SCALE: native data type scale
		 *     REMOTE_PRECISION: native data type precision
		 *     DESCRIPTION
		 *     DEFAULT_LANGUAGE: column description's default language
		 *     IS_INSERTABLE
		 *     IS_UPDATEABLE
		 *     IS_UPSERTABLE
		 *     IS_SELECTABLE
		 * 
		 * Constraints Level:
		 *     Primary Key:
		 *         UNIQUE_NAME
		 *         CONSTRAINT_NAME
		 *         COLUMN_POSITION
		 *         COLUMN_NAME
		 *         IS_PRIMARY
		 *     Foreign key:
		 *         UNIQUE_NAME
		 *         COLUMN_NAME
		 *         POSITION
		 *         CONSTRAINT_NAME
		 *         REFERENCED_UNIQUE_NAME
		 *         REFERENCED_COLUMN_NAME
		 *         REFERENCED_CONSTRAINT_NAME
		 *         UPDATE_RULE
		 *         DELETE_RULE
		 *     Index:
		 *         UNIQUE_NAME
		 *         INDEX_NAME
		 *         COLUMN_POSITION
		 *         COLUMN_NAME
		 */
		
		TableName _tableName = new TableName(table);
        final String tableName = _tableName.getTableName();
        List<Column> cols = new ArrayList<Column>();
        PreparedStatement stmt = null;
        ResultSet rsColumns = null;
        ResultSet rsContraint = null;
        StringBuffer timeStampTZCol = null;
        StringBuffer timeStampLTZCol = null;
        StringBuffer bFileCol = null;
        StringBuffer xmlTypeCol = null;
        String tableType = "UNKNOWN";
        String tableDesc = null;
        List<UniqueKey> uniqueKeys = null;
        List<ForeignKey> foreignKeys = null;
        List<Index> indices = null;
        Connection connection = getConnection();
        try {
            DatabaseMetaData meta = connection.getMetaData();

            // Query out all primary key columns.
            Set<String> primaryKeys = new HashSet<String>();
            try {
                rsColumns = meta.getPrimaryKeys(null, escapedOwner, tableName);
                String constraints = null;
                while (rsContraint.next()) {
                    constraints = rsContraint.getString(1);
                }
                
                if (constraints == null) {
                    while (rsColumns.next()) {
                        primaryKeys.add(columnName);
                    }
                }
            } 

            rsColumns = meta.getColumns(null, escapedOwner, escapedTableName , null);
            while (rsColumns.next()) {
                String columnName = rsColumns.getString("COLUMN_NAME");
                int columnType = rsColumns.getInt("DATA_TYPE");
                String columnTypeName = rsColumns.getString("TYPE_NAME");
                int size = rsColumns.getInt("COLUMN_SIZE");
                boolean nullable = rsColumns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                int charlength = rsColumns.getInt("CHAR_OCTET_LENGTH");
                int scale = rsColumns.getInt("DECIMAL_DIGITS");
                String description = rsColumns.getString("REMARKS");
                boolean isPrimaryKey = primaryKeys.contains(columnName);

                Column col = newColumnFor(owner, tableName, columnName,
                        columnType,
                        columnTypeName,
                        description,
                        size,
                        scale,
                        isPrimaryKey,
                        nullable, size,
                        (size != charlength));
                cols.add(col);
            }
            // unique keys, foreign keys, indices
            uniqueKeys = getUniqueKeys(meta, escapedOwner, owner, tableName);
            foreignKeys = getForeignKeys(meta, owner, tableName);
            indices = getIndices(meta,owner, tableName);
        } catch(Exception e) {
            String message = "Failed to import metadata due to exception." + e.getMessage();
            logger.error(message, e);
            throw new AdapterException(e, message);
        } 
        
        TableMetadata metas = new TableMetadata(cols);
        metas.setName(table);
        metas.setDatabase(remoteSourceDatabaseName);
        metas.setOwner(owner);
        metas.setPhysicalName(_tableName.getTableName());
        metas.setRemoteObjectType(tableType);
        metas.setDescription(tableDesc);
        
        // unique keys, foreign keys, indices
        metas.setUniqueKeys(uniqueKeys);
        metas.setForeignKeys(foreignKeys);
        metas.setIndices(indices);

        if(((TableMetadata) metas).getColumns() == null || ((TableMetadata) metas).getColumns().size() == 0 )
            throw new AdapterException("Adapter returned zero columns for import request. Please make sure db user : " + owner + " has select privilege on the table : " + tableName + " .");

        return metas;
	}
	
	@Override
	public void executeStatement(String sql,StatementInfo info) throws AdapterException {
		Query query = null;
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		query = (Query) ExpressionParserUtil.buildQuery(sql, messageList);
		if(query == null)
			throw new AdapterException("Parsing the sql " + sql + " failed");
		ExpressionBase sda_fromclause = query.getFromClause();
		if (sda_fromclause instanceof TableReference) {
			// Kind of useless, only one single table exists anyhow...
			tablename_to_read = ((TableReference) sda_fromclause).getUnquotedName();
		} else {
			throw new AdapterException("select does not read a single table???");
		}
		rowsread = 0;
	}

	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		if (tablename_to_read.equals(HELLO)) {
			int batchsize = 0;
			while (rowsread<10 && batchsize<this.fetchsize) {
				AdapterRow row = rows.newRow();
				/*
				 * Actually we need to check what columns had been selected.
				 * But since the AdapterCapability.CAP_PROJECT has not been set
				 * we know for sure the table structure: it is the HELLO table with the columns
				 * ROWNUMBER of type Integer
				 * TEXT of typo nvarchar(80)
				 */
				row.setColumnValue(0, rowsread);
				row.setColumnValue(1, username + " said: Hello " + name);
				rowsread++;
				batchsize++;
			}
		} else {
			// cannot happen anyhow
		}
	}

	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
		// CLOBs or BLOBs reading not required by this adapter, no table with that datatype 
		return 0;
	}

	
	@Override
	public String addSubscription(SubscriptionSpecification spec) throws AdapterException {
		// we do not deal with new subscriptions here but in start
		return spec.getSubscription();
	}
	
	@Override
	public void removeSubscription(SubscriptionSpecification spec) throws AdapterException {
		// we do not deal with new subscriptions here but in stop
	}

	@Override
	public void start(ReceiverConnection conn, SubscriptionSpecification spec) throws AdapterException {
		// remember the connection. If it was set already it does not matter, the connection is the same for all
		this.receiverconnection = conn;
		// add the new spec to the list of all currently active specs
		this.activespecs.put(spec.getSubscription(), spec);
		// Parse the spec's SQL Statement so we know what table to read from.
		// Granted, there is just one table but its the principles
		Query query = null;
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		query = (Query) ExpressionParserUtil.buildQuery(spec.getSQLStatement(), messageList);
		if(query == null)
			throw new AdapterException("Parsing the sql " + spec.getSQLStatement() + " failed");
		ExpressionBase fromclause = query.getFromClause();
		String tablename = null;
		if (fromclause instanceof TableReference) {
			// Kind of useless, only one single table exists anyhow...
			tablename = ((TableReference) fromclause).getUnquotedName();
		} else {
			throw new AdapterException("select does not read a single table???");
		}
		// We need the table metadata, actually the selected columns from the Query.
		// But since the adapter does not support projections, that is the same anyhow.
		TableMetadata m = (TableMetadata) importMetadata(tablename);
		this.activespeccolumns.put(spec.getSubscription(), m.getColumns());
		// This adapter does something every 5 seconds, we need a thread for that.
		// But one thread is enough for all subscriptions.
		if (poller == null) {
			poller = new Poller();
			poller.start();
		}
	}

	@Override
	public void stop(SubscriptionSpecification spec) throws AdapterException {
		this.activespecs.remove(spec.getSubscription());
		this.activespeccolumns.remove(spec.getSubscription());
	}
	
	@Override
	public void beginMarker(String markername, SubscriptionSpecification spec) throws AdapterException {
		if (receiverconnection != null) {
			AdapterCDCRowSet rowset = AdapterAdmin.createBeginMarkerRowSet(markername, new SequenceId(System.currentTimeMillis()));
			receiverconnection.sendRowSet(rowset);
		} else
			throw new AdapterException("begin marker requested for a non-active subscription???");
	}

	@Override
	public void endMarker(String markername, SubscriptionSpecification spec) throws AdapterException {
		if (receiverconnection != null) {
			AdapterCDCRowSet rowset = AdapterAdmin.createEndMarkerRowSet(markername, new SequenceId(System.currentTimeMillis()));
			receiverconnection.sendRowSet(rowset);
		} else
			throw new AdapterException("end marker requested for a non-active subscription???");
	}

	@Override
	public boolean requireDurableMessaging() {
		return true;
	}

	@Override
	public boolean supportsRecovery() {
		return false;
	}
	
	@Override
	public void committedChange(SubscriptionSpecification spec)	throws AdapterException {
	}
	
	@Override
	public void beginTransaction() throws AdapterException {
	}

	@Override
	public void commitTransaction() throws AdapterException {
	}

	@Override
	public void rollbackTransaction() throws AdapterException {
	}

	class Poller extends Thread {
		
		@Override
		public void run() {
			int rowcount = 0;
			while (isInterrupted() == false) {
				try {
					TimeUnit.SECONDS.sleep( 5 );
				} catch (InterruptedException e) {
					interrupt();
				}
				if (receiverconnection != null && activespecs.size() != 0) {

					ByteBuffer buffer = ByteBuffer.allocate(8);
					buffer.putLong(System.currentTimeMillis());
					byte[] transactionid = buffer.array();

					SequenceId sequence = new SequenceId(System.currentTimeMillis());

					try {
						for (SubscriptionSpecification spec : activespecs.values()) {
							List<Column> columns = activespeccolumns.get(spec.getSubscription());
							if (columns != null) {
								AdapterCDCRowSet rows = new AdapterCDCRowSet(spec.getHeader(), columns);
								AdapterCDCRow row = rows.newCDCRow(RowType.INSERT);
								row.setColumnValue(0, rowcount);
								row.setColumnValue(1, username + " said: Hello " + name);
								row.setTransactionId(transactionid);
								row.setSeqID(sequence);
								receiverconnection.sendRowSet(rows);
								rowcount++;
							} else {
								// cannot happen
							}
						}
						// do not send a commit row if there is no active spec
						receiverconnection.sendRowSet(AdapterAdmin.createCommitTransactionRowSet(new SequenceId(System.currentTimeMillis()), transactionid));
					} catch (AdapterException e) {
						// data was not been sent
					}
				}
			}
		}

	}

		
	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		// Ignore, not supported yet to write into Adapters
		return 0;
	}

	@Override
	public void startLatencyTicket(
			LatencyTicketSpecification latencyTicketSpecification)
			throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AdapterStatistics getAdapterStatistics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAdapterStatisticsUpdateInterval(
			int hint_update_interval_in_seconds) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int executeUpdate(String sql, StatementInfo info)
			throws AdapterException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Metadata importMetadata(String nodeId, List<Parameter> dataprovisioningParameters) throws AdapterException {
		throw new AdapterException("Table does not support data provisioning parameters");
	}

	@Override
	public ParametersResponse queryParameters(String nodeId, List<Parameter> parametersValues) throws AdapterException {
		return null;
	}

	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BrowseNode> loadTableDictionary(String lastUniqueName)
			throws AdapterException {
		/**
		 * This is a faster way to load table metadata as searchable
		 * dictionary. The return metadata is not detailed described.
		 * 
		 * Remember to apply filters is existing.
		 * 
		 * Required Metadata from ESS:
		 *     UNIQUE_NAME
		 *     DATABASE_NAME
		 *     OWNER_NAME
		 *     PHYSICAL_NAME
		 *     DISPLAY_NAME
		 *     OBJECT_TYPE
		 *     DESCRIPTION
		 *     DEFAULT_LANGUAGE
		 *     IS_USER_DEPENDENT
		 *     LAST_MODIFICATION_TIMESTAMP
		 */
		
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
        ResultSet schemasSet = null;
        Statement getDatabaseNameStmt = null;
        ResultSet getDatabaseNameResult = null;
        browseOffset = 0;
        Connection sdaConnection = getConnection();

        if (bulkBrowseResultSet == null) {
            try {
                DatabaseMetaData mds = sdaConnection.getMetaData();
                //get database name
                if(remoteObjectsFilter.getFilterDatabaseName().compareTo("null") != 0
                        && remoteObjectsFilter.getFilterDatabaseName().length() > 0
                        && remoteSourceDatabaseName.compareTo(remoteObjectsFilter.getFilterDatabaseName()) != 0)
                    return nodes; // apply filter here

                schemasSet = mds.getSchemas();
                RemoteObjectsFilterPatterns remoteObjectsFilterPattern = new RemoteObjectsFilterPatterns(remoteObjectsFilter);
                String searchStringEscape = mds.getSearchStringEscape();
                String ownerFilter = remoteObjectsFilterPattern.getEscapedOwnerPattern(searchStringEscape);
                String tableFilter = remoteObjectsFilterPattern.getEscapedTableNamePattern(searchStringEscape);
                bulkBrowseResultSet = mds.getTables(null, ownerFilter, tableFilter, null);
                closeJDBCHandles(schemasSet, null);
            }catch (Exception e) {
                String message = "Failed to query schemas.";
                logger.error(message, e);
                closeJDBCHandles(bulkBrowseResultSet, null);
                throw new AdapterException(e, message);
            } finally{
                closeJDBCHandles(schemasSet, null);
            }
        }

        try{
            while(bulkBrowseResultSet.next()) {
                String tableName = bulkBrowseResultSet.getString("TABLE_NAME");
                String schema = bulkBrowseResultSet.getString("TABLE_SCHEM");
                String description = bulkBrowseResultSet.getString("REMARKS");
                String tableUniqueName = "\"" + schema + "\".\"" + tableName + "\"";
                String objectType = bulkBrowseResultSet.getString("TABLE_TYPE");
                BrowseNode node = new BrowseNode(tableUniqueName, tableName);
                node.setImportable(true);
                node.setExpandable(false);
                node.setNodeType(NodeType.TABLE);
                node.setOwner(schema);
                node.setName(tableName);
                node.setDatabase(remoteSourceDatabaseName);
                node.setParentNodeId(schema);
                node.setPhysicalName(tableName);
                if(description != null && !description.isEmpty())
                    node.setDescription(description);

                try {
                       node.setLastModificationDate(lastModifiedTimeStamp);
                    }
                } catch (Exception ex) {
                    logger.warn("Failed to retrieve last modified timeStamp of table <" + schema + "." + tableName + ">:" + ex.getMessage());
                } 

            }
        } catch (Exception e) {
            String message = "Failed to query schemas.";
            logger.error(message, e);
            throw new AdapterException(e, message);
        } 
        return nodes;
	}

	@Override
	public void executePreparedInsert(String sql, StatementInfo info)
			throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void executePreparedUpdate(String sql, StatementInfo info)
			throws AdapterException {
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
		/*
		 * We treat remote objects filter as internal object, it contains filters applied
		 * on database name, owner name, physical name and unique name. 
		 * Unique name could also be represented by database name + owner name + physical
		 * name. Which also means, it is not necessary to apply filter on unique name and 
		 * other three filters at the same time. So, one or more filter(s) on database name,
		 * owner name and physical name could exist at the same time, however, unique name 
		 * filter always come alone. 
		 */
		this.remoteObjectsFilter = remoteObjectsFilter;
		
	}

	@Override
	public void validateCall(FunctionMetadata arg0) throws AdapterException {
		// TODO Auto-generated method stub
		
	}
	

}
