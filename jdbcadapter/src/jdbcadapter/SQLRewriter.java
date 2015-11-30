package jdbcadapter;

import java.util.ArrayList;
import java.util.List;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Join;
import com.sap.hana.dp.adapter.sdk.parser.Order;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;


public class SQLRewriter {

	//SQL rewrite entry point
	public static String rewriteSQL(String sql)  throws AdapterException 
	{
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
			try
			{
				ExpressionBase query = ExpressionParserUtil.buildQuery(sql, messageList);
				if (query != null)
				{
					String sqlRewrite = regenerateSQL(query);
					JDBCAdapter.logger.trace(sql);
					JDBCAdapter.logger.trace(sqlRewrite);
					return sqlRewrite;
				}
				else
				{
					for (ExpressionParserMessage e : messageList)
					{
						JDBCAdapter.logger.error(e.getText());					
					}
					throw new AdapterException("Parse failed. See earlier logs");
				}
			}
			catch ( Exception e ) {
				JDBCAdapter.logger.error("SQL Rewrite failed.", e);
				throw new AdapterException(e,"Parser failed. See earlier logs");
			}
	}

	private static String regenerateSQL(ExpressionBase query) throws Exception
	{
		if (query.getType() == Type.SELECT)
			return regenerateSQL((Query)query);
		else
		{
			StringBuffer str = new StringBuffer();
			Expression exp = (Expression) query;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" ");
			str.append(printSetOperation(query.getType()));
			str.append(" ");
			str.append(printExpression(exp.getOperands().get(1)));
			return str.toString();
		}
	}
	
	private static String regenerateSQL(Query query) throws Exception
	{
		StringBuffer sql = new StringBuffer();
		
		sql.append("SELECT ");
		if (query.getDistinct())
			sql.append("DISTINCT ");
		sql.append(printColumnList(query.getProjections()));
		sql.append(" FROM ");
		sql.append(printExpression(query.getFromClause()));
		if (query.getWhereClause() != null)
		{
			sql.append(" WHERE ");
			sql.append(printWhereClause(query.getWhereClause()));
		}
		if (query.getGroupBy() != null)
		{
			sql.append(" GROUP BY ");
			sql.append(printColumnList(query.getGroupBy()));
		}
		if (query.getHavingClause() != null)
		{
			sql.append(" HAVING ");
			sql.append(printWhereClause(query.getHavingClause()));
		}
		if (query.getOrderBy() != null)
		{
			sql.append(" ORDER BY ");
			sql.append(printOrder(query.getOrderBy()));
		}
		if (query.getLimit() != null)
		{
			sql.append(" LIMIT ");
			sql.append(query.getLimit());
		}
			
		return sql.toString();
	}
	private static String printOrder(List<Order> order) throws Exception
	{
		boolean first = true;
		StringBuffer str = new StringBuffer();
		for (Order o : order)
		{
			if (first)
				first = false;
			else
				str.append(", ");
			str.append(printExpression(o.getExpression()));
			if (o.getOrderType() == Order.Type.ASC)
				str.append(" ASC");
			else
				str.append(" DESC");
		}
		return str.toString();
	}
	private static String printColumnList(List<ExpressionBase> proj) throws Exception
	{
		boolean first = true;
		StringBuffer str = new StringBuffer();
		for (ExpressionBase exp : proj)
		{
			if (first)
				first = false;
			else
				str.append(", ");
			str.append(printExpression(exp));
		}
		return str.toString();
	}
	
	private static String printWhereClause(List<ExpressionBase> where) throws Exception
	{
		boolean first = true;
		StringBuffer str = new StringBuffer();
		for (ExpressionBase exp : where)
		{
			if (!first)
				str.append(" AND (");

			str.append(printExpression(exp));
			
			if (!first)
				str.append(")");
			
			if (first)
				first = false;
		}
		return str.toString();
	}
	
	private static String printExpression(ExpressionBase val) throws Exception
	{
		StringBuffer str = new StringBuffer();
		switch(val.getType())
		{
		case COLUMN:
		{
			ColumnReference exp = (ColumnReference)val;
			if (exp.getTableName() != null)
				str.append(exp.getTableName() + ".");
			str.append(exp.getColumnName());
			break;
		}
		case TABLE:
		{
			TableReference tab = (TableReference)val;
			str.append(tab.getName());
			break;
		}
		case SELECT:
		{
			Query query = (Query)val;
			str.append(" ( ");
			str.append(regenerateSQL(query));
			str.append(" ) ");
			break;
		}
		case QUERY:
		{
			Query query = (Query)val;
			str.append(" ( ");
			str.append(regenerateSQL(query));
			str.append(" ) ");
			break;
		}
		case INNER_JOIN:
		case LEFT_OUTER_JOIN:
		{
			Join join = (Join)val;
			str.append("(");
			str.append(printExpression(join.getLeftNode()));
			if (val.getType() == Type.INNER_JOIN)
				str.append(" INNER JOIN ");
			else
				str.append(" LEFT OUTER JOIN ");
			str.append(printExpression(join.getRightNode()));
			str.append(" ON (");
			str.append(printExpression(join.getJoinCondition()));
			str.append("))");
			break;
		}
		case ALL:
			str.append("*");
			break;
		case FUNCTION:
		{
			Expression exp = (Expression)val;
			str.append(exp.getValue() + "(");
			boolean first = true;
			for (ExpressionBase param : exp.getOperands())
			{
				if (first)
					first = false;
				else
					str.append(", ");
				str.append(printExpression(param));
			}
			str.append(")");
			break;
		}
		case AND:
		case OR:
		{
			Expression exp = (Expression)val;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" " + exp.getValue() + " ");
			str.append(printExpression(exp.getOperands().get(1)));
			break;
		}
		case IN:
		case NOT_IN:
		{
			Expression exp = (Expression)val;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" " + exp.getValue() + " (");
			str.append(printExpression(exp.getOperands().get(1)));
			int count = exp.getOperands().size();
			for (int k=2; k<count; k++)
			{
				str.append(", ");
				str.append(printExpression(exp.getOperands().get(k)));
			}
			str.append(")");
			break;
		}	
		case BETWEEN:
		case NOT_BETWEEN:
		{
			Expression exp = (Expression)val;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" " + exp.getValue() + " ");
			str.append(printExpression(exp.getOperands().get(1)));
			str.append(" AND ");
			str.append(printExpression(exp.getOperands().get(2)));
			break;
		}
		case EQUAL: 
		case NOT_EQUAL: 
		case LESS_THAN: 
		case LESS_THAN_EQ: 
		case GREATER_THAN: 
		case GREATER_THAN_EQ:
		case NOT_LIKE:
		case LIKE:
		case ADD: 
		case SUBTRACT: 
		case MULTIPLY: 
		case DIVIDE:
		{
			//binary operation
			Expression exp = (Expression)val;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" " + exp.getValue() + " ");
			str.append(printExpression(exp.getOperands().get(1)));
			break;
		}
		case IS_NULL:
		case IS_NOT_NULL:
		{
			Expression exp = (Expression)val;
			str.append(printExpression(exp.getOperands().get(0)));
			str.append(" " + exp.getValue() + " ");
			break;
		}
		case INT_LITERAL: 
		case FLOAT_LITERAL:
		{
			Expression exp = (Expression)val;
			str.append(exp.getValue());
			break;
		}
		case CHARACTER_LITERAL:
		{
			Expression exp = (Expression)val;
			str.append("N"+exp.getValue());
			break;
		}
		case UNION_ALL:
		case UNION_DISTINCT:
		case INTERSECT: 
		case EXCEPT:
			str.append(" ( ");
			str.append(regenerateSQL(val));
			str.append(" ) ");
			break;
		case UNARY_NEGATIVE:
		case UNARY_POSITIVE:
		{
			Expression exp = (Expression)val;
			str.append(exp.getValue());
			str.append(printExpression(exp.getOperands().get(0)));
			break;
		}
		default:
			Expression exp = (Expression)val;
			throw new Exception("Unknown value:" + exp.getValue());
		}
		
		if (val.getAlias() != null)
		{
			str.append(" ");
			str.append(val.getAlias());
		}
		
		return str.toString();
	}
	
	private static String printSetOperation(Type type)
	{
		String str = new String();
		switch (type)
		{
		case UNION_ALL:
			str = "UNION ALL";
			break;
		case UNION_DISTINCT:
			str = "UNION DISTINCT";
			break;
		case INTERSECT:
			str = "INTERSECT";
			break;			
		case EXCEPT:
			str = "EXCEPT";
			break;
		default:
			break;
		}
		return str;
	}
}
