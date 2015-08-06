package helloworldadapter2;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;

public class TableLoaderHelloWorldAdapter2 extends TableLoader {

	public TableLoaderHelloWorldAdapter2(BaseAdapterClass adapter) {
		super(adapter);
	}

	@Override
	protected Object getNextRowData(AdapterRowSet rows) throws AdapterException {
		if (getRowNumberToRead() < 10) {
			// return an object that can be used to parse the return values of the row.
			// anything as long as it is not null
			return getRowNumberToRead();
		} else {
			hasNoMoreRows();
			return null;
		}
	}

	@Override
	protected void setColumnValue(int tablecolumnindex, int returncolumnindex, AdapterRow row, Object o) throws AdapterException {
    	switch (tablecolumnindex) {
    	case 0:
    		row.setColumnValue(returncolumnindex, (Integer) o);
    		break;
    	case 1:
    		row.setColumnValue(returncolumnindex, ((HelloWorldAdapter2) getAdapter()).username + " said: Hello " + ((HelloWorldAdapter2) getAdapter()).name);
    		break;
    	}		
	}

	@Override
	public void executeStatementEnded() throws AdapterException {
		// no resources to close
	}

	public static void importMetadata(TableMetadata table) {
        table.setName(HelloWorldAdapter2.HELLO);
        table.setPhysicalName(HelloWorldAdapter2.HELLO);
        table.setDescription("Hello World table");
        
        addColumnInteger(table, "ROWNUMBER", "The row number");
        addColumnNVarchar(table, "TEXT", 80, "The Hello text");
	}

	@Override
	public void close() throws AdapterException {
	}

}
