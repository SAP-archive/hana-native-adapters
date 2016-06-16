package rssadapter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionInformationPerTablename;
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionRuntimeInformation;
import com.sap.hana.dp.adapter.sdk.adapterbase.Subscriptions;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherEvent;
import com.sun.syndication.fetcher.FetcherException;
import com.sun.syndication.fetcher.FetcherListener;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;
import com.sun.syndication.io.FeedException;

public class RSSAdapter extends BaseAdapterClass {

	private static final String URL = "URL";
	static Logger logger = LogManager.getLogger("RSSAdapter");
	private String rssurl = null;

	private FeedFetcher feedFetcher = null;
    private URL URL_object = null;
	
	@Override
	public void addRemoteSourceDescriptors(PropertyGroup root) throws AdapterException {
		root.addProperty(new PropertyEntry(URL, URL));
	}
	
	public void addRemoteSourceCredentialDescriptors(CredentialProperties credential) throws AdapterException {
		// No username or password to be used
	}

	@Override
	public void open(RemoteSourceDescription arg0, boolean arg1) throws AdapterException {
		rssurl = getPropertyValueByPath(arg0, URL);
	}

	@Override
	public void addNodes(List<BrowseNode> nodes) throws AdapterException {
		nodes.add(createNewTableBrowseNode(TableLoaderRSSFeed.RSSFEED, TableLoaderRSSFeed.RSSFEED, "RSS Feed Table"));
	}

	@Override
	public void importMetadata(ArrayList<String> fullIDStringToLevels, TableMetadata table, List<Parameter> dataprovisioningParameters) throws AdapterException {
		String tablename = fullIDStringToLevels.get(fullIDStringToLevels.size()-1);
		if (tablename != null && tablename.equals(TableLoaderRSSFeed.RSSFEED)) {
			TableLoaderRSSFeed.importMetadata(table);
		} else {
			throw new AdapterException("Unknown tablename");
		}
	}
	
	@Override
	protected TableLoader getTableLoader(String tableName, StatementInfo info) throws AdapterException {
		return new TableLoaderRSSFeed(this, tableName);
	}

	@Override
	public void executeStatement(TableLoader tl) throws AdapterException {
		((TableLoaderRSSFeed) tl).start(rssurl);
	}
		
	@Override
	public void close() throws AdapterException {
	}
	
	@Override
	public AdapterCapability[] getSDACapabilites() {
		// This capabilities indicate that this is a real time adapter, hence should be set by the BaseAdapterClass
		// As a temporary workaround, add it here explicitly.
		AdapterCapability[] caps = {AdapterCapability.CAP_TRANSACTIONAL_CDC};
		return caps; 
	}	

	@Override
	protected void startSubscription(SubscriptionRuntimeInformation subscriptionruntime) throws AdapterException {
	}	

	@Override
	public int getPollingInterval() {
		return 10;
	}

	@Override
	public void poll() {
		try {
			feedFetcher.retrieveFeed(URL_object);
		} catch (IllegalArgumentException | IOException | FeedException | FetcherException e) {
			// TODO Proper error handling
			e.printStackTrace();
		}	
	}

	@Override
	public void pollStart() throws AdapterException {
		try {
			URL_object = new URL(rssurl);
		} catch (MalformedURLException e) {
			throw new AdapterException(e);
		}
		FeedFetcherCache feedInfoCache = HashMapFeedInfoCache.getInstance();
	    feedFetcher = new HttpURLFeedFetcher(feedInfoCache);
        FetcherEventListenerImpl listener = new FetcherEventListenerImpl();
        feedFetcher.addFetcherEventListener(listener);
	}

	@Override
	public void pollEnd() throws AdapterException {
		feedFetcher = null;
		
	}

	
	class FetcherEventListenerImpl implements FetcherListener {
		public void fetcherEvent(FetcherEvent event) {
			String eventType = event.getEventType();
			if (FetcherEvent.EVENT_TYPE_FEED_POLLED.equals(eventType)) {
			} else if (FetcherEvent.EVENT_TYPE_FEED_RETRIEVED.equals(eventType)) {
				SyndFeed f = event.getFeed();
		        try {
				    for (Object e : f.getEntries()) {
						addCDCRow("RSSFEED", e, RowType.UPSERT);
				    }
				    sendRows();
					commit();
				} catch (AdapterException e1) {
					// If the adapter instance lost contact with Hana, the subscriptions have to be removed. 
					e1.printStackTrace();
					try {
						Subscriptions s = getSubscriptions();
						SubscriptionInformationPerTablename sub = s.get("RSSFEED");
						if (sub != null) {
							for (SubscriptionRuntimeInformation rs : sub.getSubscriptionList().values()) {
								stop(rs.getSubscriptionSpecification());
							}
						}
					} catch (AdapterException e) {
					}
				}
			} else if (FetcherEvent.EVENT_TYPE_FEED_UNCHANGED.equals(eventType)) {
			}
		}
	}

	@Override
	protected void stopSubscrition(SubscriptionSpecification subscription) {
	}

}
