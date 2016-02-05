package jdbcadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class JDBCAdapterFactory implements AdapterFactory{

	
	@Override
	public Adapter createAdapterInstance() {
		return new JDBCAdapter();
	}

	@Override
	public String getAdapterType() {
		return "JDBCAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return "Generic Database Adapter";
	}

	@Override
	public String getAdapterDescription() {
		return "Generic Database Adapter";
	}

	@Override
	public RemoteSourceDescription getAdapterConfig() {
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

}
