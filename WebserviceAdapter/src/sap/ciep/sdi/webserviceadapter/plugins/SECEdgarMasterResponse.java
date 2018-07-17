package sap.ciep.sdi.webserviceadapter.plugins;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;

import sap.ciep.sdi.webserviceadapter.WebserviceResponseHandler;
import sap.ciep.sdi.webserviceadapter.WebserviceResponseRecord;

public class SECEdgarMasterResponse extends WebserviceResponseHandler {

	static final int SKIP_LINES=12; 
	static final String FIELD_SEPARATOR="\\|";
	
	@Override
	public LinkedList<WebserviceResponseRecord> handleResponse(CloseableHttpResponse r, String url)
			throws AdapterException, ParseException, IOException {
		
		BufferedReader in = new BufferedReader(new InputStreamReader(r.getEntity().getContent()));
		String line;
		String[] fields;
		
		LinkedList<WebserviceResponseRecord> res = new LinkedList<WebserviceResponseRecord>();
		
		for(int i=0;i<SKIP_LINES;i++)
			in.readLine();
				
		while((line = in.readLine()) != null) {
			fields=line.split(FIELD_SEPARATOR);
			if(fields.length==5){
				res.add(new SECEdgarMasterRecord(Integer.parseInt(fields[0]),
						fields[1],
						fields[2],
						fields[3],
						fields[4]					
						));		
			}
			else {
				logger.debug("strange row: "+line);
			}
			
		}
		in.close();
		return res;
	}
	
	/**
	 * Describes a tag
	 * @author I063909
	 *
	 */
	public static class SECEdgarMasterRecord extends WebserviceResponseRecord{

		int cik;
		String cName;
		String fType;
		String dt;
		String fName;
		public SECEdgarMasterRecord(int cik, String cName, String fType, String dt, String fName){
			this.cik=cik;
			this.cName=cName;
			this.fType=fType;
			this.dt=dt;
			this.fName=fName;
		}
		@Override
		public void appendTo(AdapterRowSet rows) throws AdapterException {
			AdapterRow row=rows.newRow();
			row.setColumnValue(0, cik);
			row.setColumnValue(1, cName);
			row.setColumnValue(2, fType);
			row.setColumnValue(3, dt);
			row.setColumnValue(4, fName);
		}
		
	}

}
