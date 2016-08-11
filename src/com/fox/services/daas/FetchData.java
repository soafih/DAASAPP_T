package com.fox.services.daas;

import java.sql.ResultSet;
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

@Path("/")
public class FetchData {

	@GET
	// @Path("/FetchData")
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Response processSelect(@Context UriInfo info) throws JSONException {

		String result = "";
		int statusCode = 200;

		try {
			String query = formQuery(info);
			JSONObject resultJson = DatabaseProxy.executeQuery(query);
			result = resultJson.toString();

		} catch (Exception ex) {
			ex.printStackTrace();
			JSONObject resultJson = getErrorJson(ex);
			result = resultJson.toString();
			statusCode = 501;

		}

		return Response.status(statusCode).entity(result).build();
	}

	private String formQuery(UriInfo info) throws Exception {

		String query = DAASAppUtil.getProperty("Query");

		Pattern patt = Pattern.compile("#[a-zA-Z0-9_]*#");
		Matcher m = patt.matcher(query);
		StringBuffer sb = new StringBuffer(query.length());
		while (m.find()) {
			String text = m.group(0);
			text = text.substring(1, text.length() - 1);

			if (info.getQueryParameters().getFirst(text) == null) {
				throw new Exception("Parameter : " + text + " is not passed");
			}

			m.appendReplacement(sb, info.getQueryParameters().getFirst(text));
		}
		m.appendTail(sb);
		return (sb.toString());

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
