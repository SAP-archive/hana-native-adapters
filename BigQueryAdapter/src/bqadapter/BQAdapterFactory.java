/**
 * (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package bqadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class BQAdapterFactory implements AdapterFactory{

	@Override
	public Adapter createAdapterInstance() {
		return new BQAdapter();
	}

	@Override
	public String getAdapterType() {
		return "BQAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return "BQAdapter";
	}

	@Override
	public String getAdapterDescription() {
		return "SAP BQAdapter Data Provisioning Adapter";
	}
	
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		return null;
	}
	
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription remoteSourceDescription)
			throws AdapterException {
		try{
		Integer.parseUnsignedInt(remoteSourceDescription
				.getConnectionProperties().getPropertyEntry("port").getValue());
		}
		catch(NumberFormatException e)
		{
			throw new AdapterException("Port number should be an unsigned integer. eg: 3306, 65000, etc.");
		}
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription arg0)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

}