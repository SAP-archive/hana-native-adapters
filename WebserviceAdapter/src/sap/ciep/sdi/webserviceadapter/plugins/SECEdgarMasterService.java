package sap.ciep.sdi.webserviceadapter.plugins;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.TableMetadata;

import java.util.ArrayList;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterLang;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.NodeType;

import sap.ciep.sdi.webserviceadapter.WebserviceConfig;


public class SECEdgarMasterService extends WebserviceConfig{

	final static String DESC="Master Index of EDGAR Dissemination Feed";
	
	public SECEdgarMasterService() {
		super("SEC_EDGAR_MASTER", 
				  DESC,
				  new SECEdgarMasterRequest(), 
				  new SECEdgarMasterResponse());
	}

	@Override
	public Metadata getMetadata() throws AdapterException {
		TableMetadata tm=new TableMetadata();
		tm.setName(name);
		tm.addDescription(AdapterLang.ENGLISH, DESC);
		ArrayList<Column> cols = new ArrayList<Column>();
		
		cols.add(new Column("CIK", DataType.INTEGER));
		cols.add(new Column("COMPANY_NAME", DataType.NVARCHAR, 128));
		cols.add(new Column("FORM_TYPE", DataType.NVARCHAR, 20));
		cols.add(new Column("DATE_FILED", DataType.NVARCHAR, 10));
		cols.add(new Column("FILE_NAME", DataType.NVARCHAR, 128));
				
		tm.setColumns(cols);
		return tm;
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.TABLE;
	}
	
}
