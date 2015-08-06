package helloworldadapter;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.sap.hana.dp.adapter.sdk.AdapterCDC;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRow;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterAdmin;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
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
import com.sap.hana.dp.adapter.sdk.SequenceId;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
*	Hello world Adapter.
*/
public class HelloWorldAdapter extends AdapterCDC {

	
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
		if (this.currentbrowsenode == null) {
			List<BrowseNode> nodes = new ArrayList<BrowseNode>();
			// list should have at max this.fetchsize elements
			// okay, in this case it is just one always
			BrowseNode node = new BrowseNode(HELLO, HELLO);
			node.setImportable(true);
			node.setExpandable(false);
			nodes.add(node);
			return nodes;
		} else {
			// Well, since there is no hierarchy all non-root nodes return zero children
			return null;
		}
	}

	
	
	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		if (nodeId.equals(HELLO)) {
			List<Column> schema = new ArrayList<Column>();
			Column col1 = new Column("ROWNUMBER", DataType.INTEGER);
			schema.add(col1);
			Column col2 = new Column("TEXT", DataType.NVARCHAR, 80);
			schema.add(col2);
			TableMetadata table = new TableMetadata();
			table.setName(nodeId);
			table.setColumns(schema);
		return table;
		} else {
			throw new AdapterException("No remote table of this name");
		}
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
		// TODO Auto-generated method stub
		return null;
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
	

}
