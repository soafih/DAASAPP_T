/**
 * 
 */
package com.fox.services.daas;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.json.JSONException;
import org.json.JSONObject;

import com.fox.services.daas.util.DAASAppUtil;

public final class DatabaseProxy {
	public static Connection createConnectionWithDS() throws NamingException, SQLException {
		
		Context ctx = new InitialContext();
		// DataSource dataSource = (DataSource) ctx.lookup(dataSourceName);
		DataSource dataSource = (DataSource) ctx.lookup("java:comp/env/" + "jdbc/DBConnection");
		Connection connection = dataSource.getConnection();

		return connection;
	}

	public static JSONObject executeQuery(String query) throws NamingException, SQLException, JSONException {
		Connection conn = null;
		Statement stmt =null;
		JSONObject resultJson=null;

		try
		{
		conn = createConnectionWithDS();
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet resultSet = stmt.executeQuery(query);
		resultJson = DAASAppUtil.processResultsAsJson(resultSet);
		}
		
		finally
		{
			DAASAppUtil.closeConnection(conn);
			DAASAppUtil.closeStatement(stmt);
		}
		
		return (resultJson);
	}

}
