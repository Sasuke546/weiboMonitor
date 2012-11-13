package com.quest.agent.weibomonitor;

import java.sql.Connection;
import java.sql.SQLException;

import java.sql.*;

public class SQLProcess {
	
	public SQLProcess(){ 
		
		try
		{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = SQLProcess.getConnection();
			
		}
		catch (Exception ex)
		{
			System.out.println("Error :" + ex.toString());
		}
	}
	
	private static String host = "jdbc:mysql://127.0.0.1:3306";
	
	private static String user = "root";
	
	private static String password = "123456";
	
	private Connection conn;
	
	public static Connection getConnection() throws SQLException{
		
		//Connection tconn = DriverManager.getConnection(host,user,password);
		
		Connection tconn = DriverManager.getConnection( "jdbc:mysql://125.216.243.51:3306/weibo?useUnicode=true&characterEncoding=gbk", "root", "123456");

		//Connection tconn = DriverManager.getConnection( "jdbc:mysql://localhost:3306/weibo?useUnicode=true&characterEncoding=utf-8", "root", "123456");

		return tconn;
	}
	
	public ResultSet executeQuery(String sqlStr) throws SQLException{
		
		if(conn==null)
			conn = getConnection();
		Statement stmt = conn.createStatement();
		
		ResultSet res = stmt.executeQuery(sqlStr);
		
		return res;
		
	}
	
public void execute(String sqlStr) throws SQLException{
		
		if(conn==null)
			conn = getConnection();
		Statement stmt = conn.createStatement();
		
		stmt.execute(sqlStr);
		
	}

}
