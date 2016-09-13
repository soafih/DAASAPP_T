package com.fox.services.daas;


import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.json.JSONException;
import org.json.JSONObject;

import com.fox.services.daas.util.DAASAppUtil;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

@Path("/")
public class FetchData {

	String query = DAASAppUtil.getProperty("Query");
	String cacheKey = DAASAppUtil.getProperty("Application");

	@GET
	// @Path("/FetchData")
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Response processSelect(@Context UriInfo info) throws JSONException {

		String result = "";
		int statusCode = 200;
		JSONObject resultJson = null;

		try {

			formQuery(info);

			System.out.println("**********Query*********" + query);
			System.out.println("**********CacheKey*******" + cacheKey);

			MemcachedClient memcachedClient = new MemcachedClient(
					AddrUtil.getAddresses(System.getenv("MEMCACHED_URL")));
			Object queryOutput = memcachedClient.get(cacheKey);
			if (queryOutput == null) {
				
				resultJson = DatabaseProxy.executeQuery(query);
				memcachedClient.add(cacheKey, Integer.parseInt(System.getenv("MEMCACHED_EXPIRY")), resultJson);

			}

			else {
				System.out.println("**********Fetching from Cache*********");
				resultJson = (JSONObject) queryOutput;
			}

			result = resultJson.toString();

		} catch (Exception ex) {
			ex.printStackTrace();
			resultJson = getErrorJson(ex);
			result = resultJson.toString();
			statusCode = 501;

		}

		return Response.status(statusCode).entity(result).build();
	}

	private void formQuery(UriInfo info) throws Exception {
		String bindVal = null;
		Pattern patt = Pattern.compile("#[a-zA-Z0-9_]*#");
		Matcher m = patt.matcher(query);
		StringBuffer sb = new StringBuffer(query.length());
		while (m.find()) {
			String text = m.group(0);
			text = text.substring(1, text.length() - 1);

			if (info.getQueryParameters().getFirst(text) == null) {
				throw new Exception("Parameter : " + text + " is not passed");
			}
			bindVal = info.getQueryParameters().getFirst(text);
			cacheKey = cacheKey + "_" + bindVal;
			m.appendReplacement(sb, bindVal);
		}
		m.appendTail(sb);
		return;

	}

	private JSONObject getErrorJson(Exception ex) throws JSONException {
		JSONObject result = new JSONObject();
		JSONObject childObject = new JSONObject();

		childObject.put("errorDetails", ex.getMessage());
		childObject.put("status", "Error");
		result.put("response", childObject);
		return result;
	}

}
