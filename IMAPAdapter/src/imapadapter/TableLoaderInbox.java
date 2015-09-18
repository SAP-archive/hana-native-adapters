package imapadapter;

import java.io.IOException;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;

import org.jsoup.Jsoup;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.adapterbase.BaseAdapterClass;
import com.sap.hana.dp.adapter.sdk.adapterbase.TableLoader;

public class TableLoaderInbox extends TableLoader {

	public TableLoaderInbox(BaseAdapterClass adapter) {
		super(adapter);
	}

	@Override
	protected Object getNextRowData(AdapterRowSet rows) throws AdapterException {
		try {
			Folder inbox = ((IMAPAdapter) getAdapter()).inbox;
			if (getRowNumberToRead() < inbox.getMessageCount()) {
				return inbox.getMessage(getRowNumberToRead()+1);
			} else {
				hasNoMoreRows();
				return null;
			}
		} catch (MessagingException e) {
			throw new AdapterException(e);
		}

	}

	@Override
	protected void setColumnValue(int tablecolumnindex, int returncolumnindex, AdapterRow row, Object o) throws AdapterException {
		Message msg = (Message) o;
		try {
	    	switch (tablecolumnindex) {
	    	case 0:
	    		Address from;
	    		if (msg.getFrom().length > 0) {
	    			from = msg.getFrom()[0];
		    		row.setColumnValue(returncolumnindex, checkLength(from.toString(), 127));
	    		} else {
		    		row.setColumnNull(returncolumnindex);
	    		}
	    		break;
	    	case 1:
	    		row.setColumnValue(returncolumnindex, checkLength(msg.getSubject(), 512));
	    		break;
	    	case 2:
	    		row.setColumnValue(returncolumnindex, new Timestamp(msg.getReceivedDate()));
	    		break;
	    	case 3:
	    		row.setColumnValue(returncolumnindex, new Timestamp(msg.getSentDate()));
	    		break;
	    	case 4:
	    		row.setColumnValue(returncolumnindex, checkLength(msg.getContentType(), 127));
	    		break;
	    	case 5:
	    		row.setColumnValue(returncolumnindex, msg.getSize());
	    		break;
	    	case 6:
	    		Address reply;
	    		if (msg.getReplyTo() != null && msg.getReplyTo().length > 0) {
	    			reply = msg.getReplyTo()[0];
	    			row.setColumnValue(returncolumnindex, checkLength(reply.toString(), 1024));
	    		} else {
	    			row.setColumnNull(returncolumnindex);
	    		}
	    		break;
	    	case 7:
	    		Object contentObj = msg.getContent();
	    		String resultString = null;
	    		if (contentObj instanceof Multipart) {
	    			BodyPart clearTextPart = null;
	    			BodyPart htmlTextPart = null;
	    			Multipart content = (Multipart)contentObj;
	    			int count = content.getCount();
	    			for(int i=0; i<count; i++)
	    			{
	    				BodyPart part =  content.getBodyPart(i);
	    				if (part.isMimeType("text/plain")) {
	    					clearTextPart = part;
	    					break;
	    				}
	    				else if(part.isMimeType("text/html")) {
	    					htmlTextPart = part;
	    				}
	    			}

	    			if (clearTextPart != null) {
	    				resultString = (String) clearTextPart.getContent();
	    			} else if (htmlTextPart != null) {
	    				String html = (String) htmlTextPart.getContent();
	    				resultString = Jsoup.parse(html).text();
	    			}

	    		} else if (contentObj instanceof String) {
	    			if (msg.getContentType().startsWith("text/html")) {
	    				resultString = Jsoup.parse((String) contentObj).text();
	    			} else {
	    				resultString = (String) contentObj;
	    			}
	    		} else {
	    			resultString = contentObj.toString();
	    		}
	    		row.setColumnValue(returncolumnindex, checkLength(resultString, 5000));
	    		break;
	    	}		
		} catch (MessagingException | IOException e) {
			throw new AdapterException(e);
		}
	}
	
	private String checkLength(String value, int maxlength) {
		if (value != null && value.length() > maxlength) {
			return value.substring(0, maxlength);
		} else {
			return value;
		}
	}

	@Override
	public void executeStatementEnded() throws AdapterException {
		// no resources to close
	}

	public static void importMetadata(TableMetadata table) {
        table.setName(IMAPAdapter.INBOX);
        table.setPhysicalName(IMAPAdapter.INBOX);
        table.setDescription("IMAP Inbox Folder");
        
        addColumnNVarchar(table, "FROM", 127, "From");
        addColumnNVarchar(table, "SUBJECT", 512, "Subject");
        addColumnTimestamp(table, "RECEIVEDATE", "Date received");
        addColumnTimestamp(table, "SENTDATE", "Date sent");
        addColumnNVarchar(table, "CONTENTTYPE", 127, "Content type");
        addColumnBigInt(table, "SIZE", "Size");
        addColumnNVarchar(table, "REPLYTO", 1024, "Reply To");
        addColumnNVarchar(table, "CONTENT", 5000, "Content");
	}

	@Override
	public void close() throws AdapterException {
	}

}
