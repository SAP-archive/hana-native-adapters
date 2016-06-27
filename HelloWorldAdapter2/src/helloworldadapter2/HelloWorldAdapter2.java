package helloworldadapter2;

import java.util.ArrayList;
import java.util.List;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionRuntimeInformation;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;

/**
*	Hello world Adapter.
*/
public class HelloWorldAdapter2 extends BaseAdapterClass {
	public static final String HELLO = "HELLO";
	private static final String CREDENTIAL = "credential";
	public String username;
	public String name;
	private int counter = 0;


	@Override
	public void addRemoteSourceDescriptors(PropertyGroup root) throws AdapterException {
		root.addProperty(new PropertyEntry("name", "Hello whom?"));
	}

	@Override
	public void addRemoteSourceCredentialDescriptors(CredentialProperties credential) throws AdapterException {
		addUserCredential(credential, CREDENTIAL, "Credentials", "Username", "Password");
	}

	@Override
	public void open(RemoteSourceDescription descriptor, boolean cdc) throws AdapterException {
		username = getUsername(descriptor, CREDENTIAL);
		@SuppressWarnings("unused")
		String password = getPassword(descriptor, CREDENTIAL);
		
		name = getPropertyValueByPath(descriptor, "name");
	}

	@Override
	public void close() throws AdapterException {
		// There are no resources or connections to be closed
	}
	
	@Override
	public void addNodes(List<BrowseNode> nodes) throws AdapterException {
		nodes.add(createNewTableBrowseNode(HELLO, HELLO, "Hello World Table"));
	}

	@Override
	public void importMetadata(ArrayList<String> fullIDStringToLevels, TableMetadata table, List<Parameter> dataprovisioningParameters) throws AdapterException {
		TableLoaderHelloWorldAdapter2.importMetadata(table);
	}


	@Override
	protected void executeStatement(TableLoader tableloader) throws AdapterException {
		// parsing happens in the BasedAdapterClass already
	}


	@Override
	protected TableLoader getTableLoader(String tableName, StatementInfo info) throws AdapterException {
		if (tableName != null && tableName.equals(HELLO)) {
			return new TableLoaderHelloWorldAdapter2(this);
		} else {
			throw new AdapterException("Unknow table");
		}
	}


	@Override
	protected void startSubscription(SubscriptionRuntimeInformation s) throws AdapterException {
	}

	@Override
	protected void stopSubscrition(SubscriptionSpecification subscription) {
	}

	@Override
	public int getPollingInterval() {
		return 5;
	}

	@Override
	public void pollStart() throws AdapterException {
		counter = 0;
	}
	
	@Override
	public void poll() {
		try {
			addCDCRow(HELLO, counter++, RowType.INSERT);
		    sendRows();
			commit();
		} catch (AdapterException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pollEnd() throws AdapterException {
	}

	@Override
	public Metadata getMetadataDetail(String arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNodesListFilter(RemoteObjectsFilter arg0) throws AdapterException {
		// TODO Auto-generated method stub
		
	}

}
