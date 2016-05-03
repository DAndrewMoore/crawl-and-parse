package com.dam_industries.dbaccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * DB establishes a connection to a database, either with default values or values sent through the set methods.
 * <p>
 * If you would like to use this code, either alter the default private values
 * or set them.
 * <p>
 * <strong>Important</strong><br>
 * You must call {@link DB#connectMe()} before you can use the object created by the default constructor.
 * 
 * @author		Andrew Moore
 * @since		1.0
 */
public class DB{
	/**
	 * 	Field that handles this instance's driver connection.
	 * 
	 *  @see	Connection
	 */
	public static Connection conn = null;
	
	/**
	 * <strong>IMPORTANT</strong>
	 * <p>
	 * You must either alter the default values or set your own
	 * in order to use this class.
	 */
	private String Username = "spiderman";
	private String Password = "MfShMhG6t6xLcUx6";
	private String host = "localhost";
	private String port = "3306";
	private String database = "crawler";
	
	/**
	 * Sets the UserName used when connecting to the database.
	 * 
	 * @param	username	The username used when connecting to the database
	 */
	public void setUserName(String username){this.Username = username;}
	
	/**
	 * Sets the Password used when connecting to the database.
	 * <p>
	 * <strong>IMPORTANTE</strong>
	 * DO NOT USE PLAIN TEXT PASSWORDS WHEN CONNECTING TO THE DATABASE.
	 * MySQL has a nifty password generator that will encrypt your password.
	 * <p>
	 * DO NOT USE THE ROOT ACCOUNT IN ANY PROGRAM OR REALLY EVER AT ALL.
	 * It is best to disable the root account all together and create users
	 * with strict permissions on what they can and can't do.
	 * <p>
	 * For example spiderman here can only access the crawler database, 
	 * but has SELECT, INSERT, UPDATE, DELETE, DROP priviledges to any 
	 * table within. Since spiderman only takes commands from the programs
	 * he's run in, you are essentially spiderman.<br>(BEWARE H@X0R$)
	 * 
	 * @param	password	The password used when connecting to the database
	 */
	public void setPassword(String password){this.Password = password;}
	
	/**
	 * Sets the SQL host.
	 * <p>
	 * If on the same machine as the server, this should stay localhost.
	 * <p>
	 * If on a different machine, either use the IP or 
	 * the Fully Qualified Domain Name (FQDN).
	 * 
	 * @param	host		The host of the SQL database
	 */
	public void setHost(String host)		{this.host = host;}
	
	/**
	 * Sets the port to connect through. Default MySQL is port 3306.
	 * <p>
	 * If you're unsure of the port, but have access to the server
	 * I'd suggest running command "sudo netstat -nlpt" or the equivalent.
	 * 
	 * @param	port		The port to connect through
	 */
	public void setPort(String port)		{this.port = port;}
	
	/**
	 * Sets the specific database as root for any queries made.
	 * <br>Effectively turns:<br>
	 * &#09;SELECT * FROM database.table WHERE 1;<br>
	 * into:<br>
	 * &#09;SELECT * FROM table WHERE 1;<br>
	 * 
	 * @param	database	The name of the database to be set as root for this connection
	 */
	public void setDataBase(String database){this.database = database;}
	
	/**
	 * Connects to the server and database with the specified arguements.
	 * <p>
	 * <strong>IMPORTANT:</strong>
	 * Must be called in order to use the DB variable created with the
	 * default constructor.
	 */
	public void connectMe(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://"+host+":"+port+"/"+database;
			conn = DriverManager.getConnection(url, Username, Password);
			System.out.println("conn built");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		if(conn != null || !conn.isClosed())
			conn.close();
	}
}