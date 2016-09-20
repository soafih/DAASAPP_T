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

import org.apache.commons.codec.digest.DigestUtils;
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
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Response processSelect(@Context UriInfo info) throws JSONException {

		String result = "";
		int statusCode = 200;
		JSONObject resultJson = null;

		try {

			formQuery(info);

			if (DAASAppUtil.getProperty("ResultCache").equals("true")) {
				System.out.println("Query Execution with result caching" );
				
				System.out.println("****Start-Time**** " + System.currentTimeMillis());
				MemcachedClient memcachedClient = new MemcachedClient(
						AddrUtil.getAddresses(System.getenv("MEMCACHED_URL")));
				
				if(cacheKey.length() > 40)
				{
					cacheKey = DigestUtils.sha1Hex(cacheKey);
				}
				
				Object queryOutput = memcachedClient.get(cacheKey);
				if (queryOutput == null) {
					System.out.println("Result Cache :"+cacheKey+ " not found" );
					resultJson = DatabaseProxy.executeQuery(query);
					memcachedClient.add(cacheKey, Integer.parseInt(DAASAppUtil.getProperty("CacheExpiry")),
							resultJson.toString());
					result = resultJson.toString();
				}

				else {
					System.out.println("Result Cache : Fetching results from cache for key "+cacheKey);
					result = (String) queryOutput;
				}
				System.out.println("****End-Time**** " + System.currentTimeMillis());

				memcachedClient.shutdown();
			}
			else
			{
				
				resultJson = DatabaseProxy.executeQuery(query);
				result = resultJson.toString();
			}

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
		query = sb.toString();
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
