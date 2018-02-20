package demoadapter;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.ScalarParameter;

public class DemoProcedureCall extends CallableProcedure {
    ProcedureMetadata metadata;
    boolean sent;
    
	public DemoProcedureCall(ProcedureMetadata metadata) {
		this.metadata = metadata;
        this.sent = false;
	}
	@Override
	public void executeCall() throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getNextRowSet(int arg0, AdapterRowSet arg1) throws AdapterException {
		if(sent)
            return;
        AdapterRow row = arg1.newRow();
        int userInput = Integer.parseInt(metadata.getParameter(0).getValue());
        row.setColumnValue(0, userInput);
              
        if(userInput > 5)
            row.setColumnValue(1, " is > 5");
        else
            row.setColumnValue(1, " is < 5");
        
        sent = true;
	}

	@Override
	public void getNextScalar(int arg0, ScalarParameter arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void putNextRowSet(int arg0, AdapterRowSet arg1) throws AdapterException {
		// TODO Auto-generated method stub

	}

}
