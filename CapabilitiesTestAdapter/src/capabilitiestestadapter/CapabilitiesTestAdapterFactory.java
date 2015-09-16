package capabilitiestestadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class CapabilitiesTestAdapterFactory implements AdapterFactory{

	@Override
	public Adapter createAdapterInstance() {
		return new CapabilitiesTestAdapter();
	}

	@Override
	public String getAdapterType() {
		return "CapabilitiesTestAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return "CapabilitiesTestAdapter";
	}

	@Override
	public String getAdapterDescription() {
		return "DP Adapter CapabilitiesTestAdapter";
	}
	
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		return null;
	}
	
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup) throws AdapterException {
		return null;
	}
}
