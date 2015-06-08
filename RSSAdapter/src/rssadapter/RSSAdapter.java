package rssadapter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
import com.sap.hana.dp.adapter.sdk.adapterbase.SubscriptionRuntimeInformation;
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

	// private static final String PROXYPORT = "proxyport";
	// private static final String PROXYHOST = "proxyhost";
	private static final String URL = "URL";
	static Logger logger = LogManager.getLogger("RSSAdapter");
	private String rssurl = null;

	private FeedFetcher feedFetcher = null;
    private URL URL_object = null;
	
	@Override
	public void addRemoteSourceDescriptors(PropertyGroup root) throws AdapterException {
		root.addProperty(new PropertyEntry(URL, URL));
		// root.addProperty(new PropertyEntry(PROXYHOST, PROXYHOST));
		// root.addProperty(new PropertyEntry(PROXYPORT, PROXYPORT));
	}
	
	public void addRemoteSourceCredentialDescriptors(CredentialProperties credential) throws AdapterException {
		// No username or password to be used
	}

	@Override
	public void open(RemoteSourceDescription arg0, boolean arg1) throws AdapterException {
		rssurl = getPropertyValueByPath(arg0, URL);
		/* String proxyhost = getPropertyValueByPath(arg0, PROXYHOST);
		String proxyport = getPropertyValueByPath(arg0, PROXYPORT);
		if (proxyhost != null && proxyhost.length() > 0 && proxyport != null) {
		    System.setProperty("proxyset", "true");
		    System.setProperty("proxyHost", proxyhost);
		    System.setProperty("proxyPort", proxyport);
		} */
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
			throw new AdapterException("Unknow tablename");
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
				System.err.println("\tEVENT: Feed Polled. URL = " +
						event.getUrlString());
			} else if (FetcherEvent.EVENT_TYPE_FEED_RETRIEVED.equals(eventType)) {
				SyndFeed f = event.getFeed();
		        try {
				    for (Object e : f.getEntries()) {
						addCDCRow("RSSFEED", e, RowType.UPSERT);
				    }
				    sendRows();
					commit();
				} catch (AdapterException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else if (FetcherEvent.EVENT_TYPE_FEED_UNCHANGED.equals(eventType)) {
				System.err.println("\tEVENT: Feed Unchanged. URL = " + event.getUrlString());
			}
		}
	}

	@Override
	protected void stopSubscrition(SubscriptionSpecification subscription) {
	}

}
