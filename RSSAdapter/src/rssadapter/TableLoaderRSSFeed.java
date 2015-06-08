package rssadapter;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherException;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;
import com.sun.syndication.io.FeedException;

public class TableLoaderRSSFeed extends TableLoader {

	public static final String RSSFEED = "RSSFEED";
	private SyndFeed feed;
	private Iterator<SyndEntry> feediterator;

	public TableLoaderRSSFeed(BaseAdapterClass adapter, String table) {
		super(adapter);
	}
	
	@Override
    protected boolean buildCDCDiff(String tablename) {
    	return false;
    }

	@Override
	protected Object getNextRowData(AdapterRowSet rows) throws AdapterException {
    	SyndEntry entry = feediterator.next();
    	if (feediterator.hasNext() == false) {
    		hasNoMoreRows();
    	}
    	return entry;
	}

	@Override
	protected void setColumnValue(int tablecolumnindex, int returncolumnindex, AdapterRow row, Object o) throws AdapterException {
		SyndEntry entry = (SyndEntry) o; 
		//        col.setName("URI");
		//        col.setName("AUTHOR");
		//        col.setName("LINK");
		//        col.setName("PUBLISHEDDATE");
		//        col.setName("TITLE");
		//        col.setName("UPDATEDATE");
		//        col.setName("DESCRIPTION");
    	switch (tablecolumnindex) {
    	case 0:
    		row.setColumnValue(returncolumnindex, entry.getUri());
    		break;
    	case 1:
    		row.setColumnValue(returncolumnindex, entry.getAuthor());
    		break;
    	case 2:
    		row.setColumnValue(returncolumnindex, entry.getLink());
    		break;
    	case 3:
			if (entry.getPublishedDate() != null)
				row.setColumnValue(returncolumnindex, new Timestamp(entry.getPublishedDate()));
			else
				row.setColumnNull(returncolumnindex);
    		break;
    	case 4:
    		row.setColumnValue(returncolumnindex, entry.getTitle());
    		break;
    	case 5:
			if (entry.getUpdatedDate() != null)
				row.setColumnValue(returncolumnindex, new Timestamp(entry.getUpdatedDate()));
			else
				row.setColumnNull(returncolumnindex);
    		break;
    	case 6:
			if (entry.getDescription() != null)
				row.setColumnValue(returncolumnindex, entry.getDescription().getValue());
			else
				row.setColumnNull(returncolumnindex);
    		break;
    	}
	}

	@Override
	public void executeStatementEnded() {
		feediterator = null;
		feed = null;
	}
	
	public static void importMetadata(TableMetadata table) throws AdapterException {
        table.setName("RSSFEED");
        table.setPhysicalName("RSSFEED");
        table.setDescription("The RSSFeed Contents");
        
        addPKColumnVarchar(table, "URI", 512, "The unique ID of the post", null, null);
        addColumnVarchar(table, "AUTHOR", 255, "Author of the RSS entry", null, null);
        addColumnVarchar(table, "LINK", 512, "Link to the post", null, null);
        addColumnTimestamp(table, "PUBLISHEDDATE", "Date published", null, null);
        addColumnVarchar(table, "TITLE", 255, "Title of the post", null, null);
        addColumnTimestamp(table, "UPDATEDATE", "Update date", null, null);
        addColumnVarchar(table, "DESCRIPTION", 1024, "Excerpt from the post", null, null);
	}

	@SuppressWarnings("unchecked")
	public void start(String rssurl) throws AdapterException {
	    FeedFetcher feedFetcher = new HttpURLFeedFetcher();
		try {
			feed = feedFetcher.retrieveFeed(new URL(rssurl));
		    feediterator = (Iterator<SyndEntry>) feed.getEntries().iterator();
		} catch (IllegalArgumentException | IOException	| FeedException | FetcherException e) {
			feediterator = null;
			feed = null;
			throw new AdapterException(e);
		}
	}

	@Override
	public void close() throws AdapterException {
	}

}
