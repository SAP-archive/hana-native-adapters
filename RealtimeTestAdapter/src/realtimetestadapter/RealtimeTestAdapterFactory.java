package realtimetestadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class RealtimeTestAdapterFactory implements AdapterFactory{

	@Override
	public Adapter createAdapterInstance() {
		return new RealtimeTestAdapter();
	}

	@Override
	public String getAdapterType() {
		// TODO Auto-generated method stub
		return "RealtimeTestAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		// TODO Auto-generated method stub
		return "RealtimeTestAdapter";
	}

	@Override
	public String getAdapterDescription() {
		// TODO Auto-generated method stub
		return "DP Adapter RealtimeTestAdapter";
	}
	
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		return null;
	}
	
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription remoteSourceDescription)
			throws AdapterException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}
}
