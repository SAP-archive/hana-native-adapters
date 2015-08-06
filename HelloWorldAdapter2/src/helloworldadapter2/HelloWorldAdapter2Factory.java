package helloworldadapter2;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class HelloWorldAdapter2Factory implements AdapterFactory{

	@Override
	public Adapter createAdapterInstance() {
		return new HelloWorldAdapter2();
	}

	@Override
	public String getAdapterType() {
		// TODO Auto-generated method stub
		return "HelloWorldAdapter2";
	}

	@Override
	public String getAdapterDisplayName() {
		// TODO Auto-generated method stub
		return "HelloWorldAdapter2";
	}

	@Override
	public String getAdapterDescription() {
		// TODO Auto-generated method stub
		return "DP Adapter HelloWorldAdapter2";
	}
	
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		return null;
	}
	
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription propertyGroup) throws AdapterException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup) throws AdapterException {
		return null;
	}

}
