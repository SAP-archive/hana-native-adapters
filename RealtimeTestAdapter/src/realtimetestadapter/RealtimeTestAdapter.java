package realtimetestadapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.sap.hana.dp.adapter.sdk.AdapterAdmin;
import com.sap.hana.dp.adapter.sdk.AdapterCDC;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRow;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
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
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.SequenceId;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
*	RealtimeTestAdapter Adapter.
*/
public class RealtimeTestAdapter extends AdapterCDC {

	protected static final String MASTER = "MASTER";

	Random rand = new Random();
	ArrayList<Integer> newrows = new ArrayList<Integer>();
	private int fetchsize;
	private String currentbrowsenode;
	private String tablename_to_read;
	private ReceiverConnection receiverconnection;
	private int rowsread;
	private Thread poller;

	private SubscriptionSpecification activespec;

	private AdapterCDCRowSet beginmarker = null;
	private AdapterCDCRowSet endmarker = null;


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
		return rs;
	}
	
	
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
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
			BrowseNode node = new BrowseNode(MASTER, MASTER);
			node.setImportable(true);
			node.setExpandable(false);
			nodes.add(node);
			return nodes;
		} else {
			return null;
		}
	}

	
	
	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		if (nodeId.equals(MASTER)) {
			List<Column> schema = new ArrayList<Column>();
			Column col1 = new Column("MASTER_ID", DataType.INTEGER);
			col1.setPrimaryKey(true);
			schema.add(col1);
			Column col2 = new Column("NAME", DataType.NVARCHAR, 255);
			schema.add(col2);
			Column col3 = new Column("LASTCHANGEDATE", DataType.TIMESTAMP);
			schema.add(col3);
			Column col4 = new Column("DESCRIPTION", DataType.NVARCHAR, 255);
			schema.add(col4);
			TableMetadata table = new TableMetadata();
			table.setColumns(schema);

	        table.setName(RealtimeTestAdapter.MASTER);
	        table.setPhysicalName(RealtimeTestAdapter.MASTER);
	        table.setDescription("The MASTER table");
			
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
		if (tablename_to_read.equals(MASTER)) {
			int batchsize = 0;
			while (rowsread<1000 && batchsize<this.fetchsize) {
				AdapterRow row = rows.newRow();
				/*
				 * Actually we need to check what columns had been selected.
				 * But since the AdapterCapability.CAP_PROJECT has not been set
				 * we know for sure the table structure: it is the HELLO table with the columns
				 * ROWNUMBER of type Integer
				 * TEXT of typo nvarchar(80)
				 */
				row.setColumnValue(0, rowsread);
				row.setColumnValue(1, "Row number " + String.valueOf(rowsread));
				row.setColumnValue(2, new Timestamp(new Date()));
				row.setColumnValue(3, "Description of row " + String.valueOf(rowsread));
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
		receiverconnection = conn;
		activespec = spec;
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
		if (tablename == null || tablename.equals(MASTER) == false) {
			throw new AdapterException("Subscription to a table this adapter does not know???");
		}
		// This adapter does something every 5 seconds, we need a thread for that.
		if (poller == null) {
			poller = new Poller();
			poller.start();
		}
	}

	@Override
	public void stop(SubscriptionSpecification spec) throws AdapterException {
		activespec = null;
	}
	
	@Override
	public void beginMarker(String markername, SubscriptionSpecification spec) throws AdapterException {
		if (receiverconnection != null) {
			beginmarker = AdapterAdmin.createBeginMarkerRowSet(markername, new SequenceId(System.currentTimeMillis()));
		} else
			throw new AdapterException("begin marker requested for a non-active subscription???");
	}

	@Override
	public void endMarker(String markername, SubscriptionSpecification spec) throws AdapterException {
		if (receiverconnection != null) {
			endmarker = AdapterAdmin.createEndMarkerRowSet(markername, new SequenceId(System.currentTimeMillis()));
		} else
			throw new AdapterException("end marker requested for a non-active subscription???");
	}

	@Override
	public boolean requireDurableMessaging() {
		return false;
	}

	@Override
	public boolean supportsRecovery() {
		return true;
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
			while (isInterrupted() == false) {
				try {
					TimeUnit.SECONDS.sleep( 10 );
				} catch (InterruptedException e) {
					interrupt();
				}
				if (receiverconnection != null && activespec != null) {

					ByteBuffer buffer = ByteBuffer.allocate(8);
					buffer.putLong(System.currentTimeMillis());
					byte[] transactionid = buffer.array();

					try {
						if (beginmarker != null) {
							receiverconnection.sendRowSet(beginmarker);
							System.out.println(beginmarker.toString(true));
							beginmarker = null;
						}
						if (endmarker != null) {
							receiverconnection.sendRowSet(endmarker);
							System.out.println(endmarker.toString(true));
							endmarker = null;
						}
						AdapterCDCRowSet rows = new AdapterCDCRowSet(activespec.getHeader(), activespec.getColumns());
						
						// Update one existing row
						int master_id = rand.nextInt(1000);
						AdapterCDCRow row = rows.newCDCRow(RowType.UPSERT);
						row.setColumnValue(0, master_id);
						row.setColumnValue(1, "Row number " + String.valueOf(master_id));
						row.setColumnValue(2, new Timestamp(new Date()));
						row.setColumnValue(3, "Description of row " + String.valueOf(master_id));
						row.setTransactionId(transactionid);
						row.setSeqID(new SequenceId(System.currentTimeMillis()));
						
						
						// UPSERT new rows
						int number_inserts = rand.nextInt(10) - 7;
						for (int i=0; i<number_inserts; i++) {
							master_id = rand.nextInt(1000)+1000;
							newrows.add(master_id);
							row = rows.newCDCRow(RowType.UPSERT);
							row.setColumnValue(0, master_id);
							row.setColumnValue(1, "Row number " + String.valueOf(master_id));
							row.setColumnValue(2, new Timestamp(new Date()));
							row.setColumnValue(3, "Description of row " + String.valueOf(master_id));
							row.setTransactionId(transactionid);
							row.setSeqID(new SequenceId(System.currentTimeMillis()));
						}
						
						// DELETE some of the new rows
						if (newrows.size() > 10) {
							master_id = newrows.get(0);
							row = rows.newCDCRow(RowType.EXTERMINATE_ROW);
							row.setColumnValue(0, master_id);
							row.setColumnNull(1);
							row.setColumnNull(2);
							row.setColumnNull(3);
							row.setTransactionId(transactionid);
							row.setSeqID(new SequenceId(System.currentTimeMillis()));
							newrows.remove(0);
				
						}

						receiverconnection.sendRowSet(rows);
						
						// do not send a commit row if there is no active spec
						receiverconnection.sendRowSet(AdapterAdmin.createCommitTransactionRowSet(new SequenceId(System.currentTimeMillis()), transactionid));
						System.out.println(rows.toString(true));
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
	


}
