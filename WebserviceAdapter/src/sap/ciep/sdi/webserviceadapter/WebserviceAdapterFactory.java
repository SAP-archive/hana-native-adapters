/**
 * (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 */
package sap.ciep.sdi.webserviceadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class WebserviceAdapterFactory implements AdapterFactory{

	@Override
	public Adapter createAdapterInstance() {
		return new WebserviceAdapter();
	}

	@Override
	public String getAdapterType() {
		// TODO Auto-generated method stub
		return "WebserviceAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		// TODO Auto-generated method stub
		return "WebserviceAdapter";
	}

	@Override
	public String getAdapterDescription() {
		// TODO Auto-generated method stub
		return "DP Adapter WebserviceAdapter";
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
