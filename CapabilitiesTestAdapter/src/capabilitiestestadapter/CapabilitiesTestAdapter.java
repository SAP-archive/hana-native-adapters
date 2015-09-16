package capabilitiestestadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.*;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
*	CapabilitiesTestAdapter Adapter.
*/
public class CapabilitiesTestAdapter extends Adapter {

	static Logger logger = LogManager.getLogger("CapabilitiesTestAdapter");
	private String sql_received = null;
	private static Capabilities<AdapterCapability> capability = new Capabilities<AdapterCapability>();
	private String tablename_to_read;
	
	{
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_UPDATE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
	}
	
	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		BrowseNode node = new BrowseNode("SQLTABLE", "SQLTABLE");
		node.setImportable(true);
		node.setExpandable(false);
		nodes.add(node);
		
		node = new BrowseNode("ADAPTERCAPS", "ADAPTERCAPS");
		node.setImportable(true);
		node.setExpandable(false);
		nodes.add(node);
		
		return nodes;
	}

	@Override
	public void close() throws AdapterException {
	}

	@Override
	public void executeStatement(String sql, StatementInfo info) throws AdapterException {
		sql_received = sql;
		
		Query query = null;
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		query = (Query) ExpressionParserUtil.buildQuery(sql, messageList);
		if (query == null)
			throw new AdapterException("Parsing the sql " + sql + " failed");
		ExpressionBase sda_fromclause = query.getFromClause();
		if (sda_fromclause instanceof TableReference) {
			// Kind of useless, only one single table exists anyhow...
			tablename_to_read = ((TableReference) sda_fromclause).getUnquotedName();
		} else {
			throw new AdapterException("select does not read a single table???");
		}
	}

	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		return capability;
	}

	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
		return 0;
	}

	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		if (tablename_to_read != null) {
			if (tablename_to_read.equals("SQLTABLE")) {
				if (sql_received == null) {
					return;
				}
				
				AdapterRow row = rows.newRow();
				row.setColumnValue(0, 1); 
				row.setColumnValue(1, sql_received);
				sql_received = null;
			} else if (tablename_to_read.equals("ADAPTERCAPS")) {
				for (AdapterCapability cap : AdapterCapability.values()) {
					AdapterRow row = rows.newRow();
					row.setColumnValue(0, cap.name());
					if (capability.hasCapability(cap)) {
						if (cap != AdapterCapability.CAP_SELECT &&
								cap != AdapterCapability.CAP_UPDATE &&
								cap != AdapterCapability.CAP_WHERE &&
								cap != AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE) {
							row.setColumnValue(1, "TRUE");
						} else {
							// These capabilities cannot be set to false
							row.setColumnValue(1, "TRUE*");
						}
					} else {
						row.setColumnValue(1, "FALSE");
					}
				}
			}
		}
		tablename_to_read = null;
	}

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		
		/* PropertyGroup connectionInfo = new PropertyGroup("testParam","Test Parameters","Test Parameters");
		connectionInfo.addProperty(new PropertyEntry("name", "name"));
		
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "Test Credentials");
		credential.getUser().setDisplayName("Demo Username");
		credential.getPassword().setDisplayName("Demo Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo); */
		return rs;
	}
	

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		return null;
	}

	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {
		List<Column> schema = new ArrayList<Column>();
		TableMetadata table = new TableMetadata();
		if (nodeId.equals("SQLTABLE")) {
			Column col1 = new Column("ROWNUMBER", DataType.INTEGER);
			col1.setNullable(true);	
			schema.add(col1);
			
			Column col2 = new Column("SQLTEXT", DataType.VARCHAR, 5000);
			col2.setNullable(true);
			schema.add(col2);
		} else if (nodeId.equals("ADAPTERCAPS")) {
			Column col1 = new Column("CAPABILITY", DataType.VARCHAR, 127);
			col1.setNullable(true);	
			schema.add(col1);
			
			Column col2 = new Column("ISSET", DataType.VARCHAR, 5);
			col2.setNullable(true);
			schema.add(col2);
		} else {
			throw new AdapterException("Wrong table name???");
		}
		
		table.setName(nodeId);
		table.setColumns(schema);
		return table;
	}

	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
	}
		
	
	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		return 0;
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
	
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
	}
	
	@Override
	public void setFetchSize(int fetchSize) {
	}
	
	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
	}
	
	@Override
	public void executePreparedInsert(String arg0, StatementInfo arg1) throws AdapterException {
	}

	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
	}

	@Override
	public int executeUpdate(String sql, StatementInfo info) throws AdapterException {
		Query query = null;
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		query = (Query) ExpressionParserUtil.buildQuery(sql, messageList);
		if (query == null)
			throw new AdapterException("Parsing the sql " + sql + " failed");
		ExpressionBase sda_fromclause = query.getFromClause();
		if (sda_fromclause instanceof TableReference) {
			String tablename = ((TableReference) sda_fromclause).getUnquotedName();
			if (tablename.equals("ADAPTERCAPS")) {
				Boolean issetvalue = null;
				List<ExpressionBase> projections = query.getProjections();
				ExpressionBase p0 = projections.get(0);
				if (p0 instanceof ColumnReference) {
					ColumnReference c0 = (ColumnReference) p0;
					if (c0.getUnquotedColumnName().equals("ISSET")) {
						Expression e0 = c0.getColumnValueExp();
						String value =  e0.getValue();
						if (value.equalsIgnoreCase("'TRUE'") || value.equals("1") || value.equals("'1'")) {
							issetvalue = true;
						} else if (value.equalsIgnoreCase("'FALSE'") || value.equals("0") || value.equals("'0'")) {
							issetvalue = false;
						}
					}
				}
				if (issetvalue != null) {
					List<ExpressionBase> where = query.getWhereClause();
					if (where.size() == 0) {
						// No where clause, so all capabilities are impacted
						int counter = 0;
						if (issetvalue.booleanValue()) {
							for (AdapterCapability cap : AdapterCapability.values()) {
								capability.setCapability(cap);
								counter++;
							}
						} else {
							for (AdapterCapability cap : AdapterCapability.values()) {
								if (cap != AdapterCapability.CAP_SELECT &&
										cap != AdapterCapability.CAP_UPDATE &&
										cap != AdapterCapability.CAP_WHERE &&
										cap != AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE) {
									capability.clearCapability(cap);
									counter++;
								}
							}
						}
						return counter;
					} else if (where.get(0) instanceof Expression) {
						Expression w0 = (Expression) where.get(0);
						if (w0.getType() == Type.EQUAL) {
							List<ExpressionBase> operands = w0.getOperands();
							if (operands.size() == 2 && operands.get(0) instanceof ColumnReference && operands.get(1) instanceof Expression) {
								ColumnReference c0 = (ColumnReference) operands.get(0);
								Expression e0 = (Expression) operands.get(1);
								if (c0.getUnquotedColumnName().equals("CAPABILITY")) {
									String v = e0.getValue();
									if (v.startsWith("'")) {
										v = v.substring(1, v.length()-1);
									}
									AdapterCapability newcap = AdapterCapability.valueOf(v);
									if (newcap != AdapterCapability.CAP_SELECT &&
											newcap != AdapterCapability.CAP_UPDATE &&
											newcap != AdapterCapability.CAP_WHERE &&
											newcap != AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE) {
										if (issetvalue.booleanValue()) {
											capability.setCapability(newcap);
											return 1;
										} else {
											capability.clearCapability(newcap);
											return 1;
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return 0;
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
	public List<BrowseNode> loadTableDictionary(String lastUniqueName) throws AdapterException {
		return null;
	}
	
	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		return null;
	}


}
