package sap.ciep.sdi.webserviceadapter;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;

import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;
import com.sap.hana.dp.adapter.sdk.AdapterException;

public abstract class JsonResponse extends WebserviceResponseHandler {

	static Gson gson = new Gson();
	
	public LinkedList<WebserviceResponseRecord> handleResponse(CloseableHttpResponse r, String url) throws AdapterException, ParseException, IOException {
		String body=EntityUtils.toString(r.getEntity());	
		logger.debug("json parsing of: \n"+body);
		JsonReader jr = new JsonReader(new StringReader(body));		
		JsonElement json = gson.fromJson(jr, JsonElement.class);
		
		if(json!=null && !json.isJsonNull())
			return parseJson(json, url);
	
		return null;
	}

	public abstract LinkedList<WebserviceResponseRecord> parseJson(JsonElement e, String url);
}
