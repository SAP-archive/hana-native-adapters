package imapadapter;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.event.MessageCountAdapter;
import javax.mail.event.MessageCountEvent;

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
import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionInformationPerTablename;
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionRuntimeInformation;
import com.sap.hana.dp.adapter.sdk.adapterbase.Subscriptions;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;


/**
*	Hello world Adapter.
*/
public class IMAPAdapter extends BaseAdapterClass {
	
	protected static final String INBOX = "INBOX";
	private static final String IMAPHOST = "IMAPHost";
	private static final String CREDENTIAL = "CREDENTIAL";
	protected Folder inbox = null;
	private Store store = null;
	private IMAPMessageListener messagelistener = null;

	@Override
	public void addRemoteSourceDescriptors(PropertyGroup root) throws AdapterException {
		root.addProperty(new PropertyEntry(IMAPHOST, "IMAP host name"));
	}

	@Override
	public void addRemoteSourceCredentialDescriptors(CredentialProperties credential) throws AdapterException {
		addUserCredential(credential, CREDENTIAL, "Credentials", "E-Mail Address", "Password");
	}

	@Override
	public void open(RemoteSourceDescription descriptor, boolean cdc) throws AdapterException {
		String emailname = getUsername(descriptor, CREDENTIAL);
		String password = getPassword(descriptor, CREDENTIAL);
		
		String imaphost = getPropertyValueByPath(descriptor, IMAPHOST);
		
		Properties props = new Properties();
		props.setProperty("mail.store.protocol", "imaps");
		props.put("mail.imaps.ssl.trust", "*");
		
		Session session = Session.getInstance(props, null);
		try {
			store  = session.getStore();
			store.connect(imaphost, emailname, password);
			inbox  = store.getFolder(INBOX);
			inbox.open(Folder.READ_ONLY);
		} catch (MessagingException e) {
			throw new AdapterException(e);
		}
	}

	@Override
	public void close() throws AdapterException {
		if (store != null) {
			try {
				store.close();
			} catch (MessagingException e) {
				throw new AdapterException(e);
			} finally {
				store = null;
				inbox = null;
			}
		}
	}
	
	@Override
	public void addNodes(List<BrowseNode> nodes) throws AdapterException {
		nodes.add(createNewTableBrowseNode(INBOX, INBOX, "IMAP Inbox"));
	}

	@Override
	public void importMetadata(ArrayList<String> fullIDStringToLevels, TableMetadata table, List<Parameter> dataprovisioningParameters) throws AdapterException {
		TableLoaderInbox.importMetadata(table);
	}


	@Override
	protected void executeStatement(TableLoader tableloader) throws AdapterException {
		// parsing happens in the BasedAdapterClass already
	}


	@Override
	protected TableLoader getTableLoader(String tableName, StatementInfo info) throws AdapterException {
		if (tableName != null && tableName.equals(INBOX)) {
			return new TableLoaderInbox(this);
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
		return 30;
	}

	@Override
	public void pollStart() throws AdapterException {
		messagelistener = new IMAPMessageListener();
		inbox.addMessageCountListener(messagelistener);
	}

	@Override
	public void poll() {
		try {
			if (inbox != null) {
				inbox.getMessageCount();
			}
		} catch (MessagingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pollEnd() throws AdapterException {
		if (inbox != null) {
			inbox.removeMessageCountListener(messagelistener);
		}
		messagelistener = null;
	}

	private class IMAPMessageListener extends MessageCountAdapter {
		public void messagesAdded(MessageCountEvent ev) {
			Message[] msgs = ev.getMessages();
			try {
				for (int i = 0; i < msgs.length; i++) {
					addCDCRow(INBOX, msgs[i], RowType.INSERT);
					msgs[i].writeTo(System.out);
				}
				commit();
			} catch (IOException | MessagingException | AdapterException e) {
				e.printStackTrace();
				try {
					Subscriptions s = getSubscriptions();
					SubscriptionInformationPerTablename sub = s.get("RSSFEED");
					if (sub != null) {
						for (SubscriptionRuntimeInformation rs : sub.getSubscriptionList().values()) {
							stop(rs.getSubscriptionSpecification());
						}
					}
				} catch (AdapterException e1) {
				}
			}
		}
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
