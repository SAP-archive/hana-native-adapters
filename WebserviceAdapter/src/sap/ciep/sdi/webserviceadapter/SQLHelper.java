package sap.ciep.sdi.webserviceadapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

public class SQLHelper {

	private String sql;
	private Type sqlType;
    private final HashMap<String, String> whereClause = new HashMap<String, String>();
    private final HashMap<String, String> updateSetClause = new HashMap<String, String>();
	private List<String> tables;

	public SQLHelper(String sql) throws AdapterException{
		this.sql = sql;
		tables = new ArrayList<String>();	
		parse();
	}
	
	private void parse() throws AdapterException{
		if(sql.isEmpty())
			throw new AdapterException("Empty SQL Statement received");
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		Query query = (Query)ExpressionParserUtil.buildQuery(sql, messageList);
		if(query == null)
			throw new AdapterException("Parse failed");
		if(query.getFromClause() instanceof TableReference) {
			 tables.add(getTableHelper(query));
		}
		else {
			Expression exp = (Expression)query.getFromClause();
			List<ExpressionBase> expList = exp.getOperands();
			for(ExpressionBase ex : expList) {
				tables.add(getTableHelper((Query)ex));
			}
		}
	}

	private String getTableHelper(Query query) throws AdapterException{
		if(query == null)
			throw new AdapterException("Parse failed");
		TableReference tableRef = (TableReference) query.getFromClause();
		sqlType = query.getType();
		String tableName = tableRef.getName().toLowerCase(); 
		if (tableName.startsWith("\"") && tableName.endsWith("\""))
			tableName = tableName.substring(1, tableName.length()-1);
		if (tableName.contains("\"\""))
			tableName = tableName.replaceAll("\"\"", "\"");
		if (sqlType == ExpressionBase.Type.DELETE
				|| sqlType == ExpressionBase.Type.SELECT
				|| sqlType == ExpressionBase.Type.UPDATE) {
			whereClause.clear();
			if(query.getWhereClause() != null){
				Expression exp1 = null;
				Expression exp2 = null;
				ColumnReference colRef1 = null;
				String whereColumn = null;
				String whereValue = null;
				for (int i = 0; i < query.getWhereClause().size(); i++) {
					exp1 = (Expression) query.getWhereClause().get(0);
					if(exp1.getOperands().get(0) instanceof ColumnReference){
						colRef1 = (ColumnReference) exp1.getOperands().get(0);
						whereColumn = colRef1.getColumnName();
						if (whereColumn.startsWith("\"") && whereColumn.endsWith("\""))
							whereColumn = whereColumn.substring(1, whereColumn.length() - 1);
						exp2 = (Expression) exp1.getOperands().get(1);
						whereValue = exp2.getValue();
						whereClause.put(whereColumn, whereValue);
					}
				}
			}
		}
		if(sqlType == ExpressionBase.Type.UPDATE && query.getProjections()!=null){
			updateSetClause.clear();
			ColumnReference colRef2 = (ColumnReference)query.getProjections().get(0);
			String setColumn = colRef2.getColumnName();
			if (setColumn.startsWith("\"") && setColumn.endsWith("\""))
				setColumn = setColumn.substring(1, setColumn.length() - 1);
			Expression exp3 = colRef2.getColumnValueExp();
			String setValue = exp3.getValue();
			updateSetClause.put(setColumn, setValue);
		}
		return tableName;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}
}
