package com.dam_industries.dbaccess;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * HelperClass is a retrieval class meant to assist with retrieving records from a database and converting them into ArrayLists.
 * <p>
 * This class has been synchronized to the database connection object to allow multithreading.
 * 
 * @author		Andrew Moore
 * @since		1.0
 */
public class HelperClass {
	
	/**
	 * Returns all records from the specified database.table in column 2 as ArrayList of type String.
	 * <p>
	 * Synchronized on the database connection object.
	 * 
	 * @param	absTablePath	The absolute database.table path to requested 
	 * @param 	column 			The column going to be pulled
	 * @return	ArrayList		An ArrayList of Strings of all records in the table of the specified path
	 * @see		ArrayList
	 * @see		PreparedStatement
	 * @see		ResultSet
	 */
	public static ArrayList<String> getRecords(String absTablePath, String column) throws SQLException{
		ArrayList<String> records = new ArrayList<String>(); //Create records array list for trash collecting...ness
		
		synchronized(DB.conn){
			PreparedStatement toCrawl = DB.conn.prepareStatement("SELECT * FROM "+absTablePath, Statement.RETURN_GENERATED_KEYS);
			ResultSet rs = toCrawl.executeQuery(); //Get URLs from our records
			
			for(int i=1; rs.absolute(i); i++) //Put them into an arraylist
				records.add(rs.getString(column));
			toCrawl.closeOnCompletion(); //Trash collecting, closing prepared statements also closes result sets
		}
		
		return records;
	}
	
	/**
	 * Returns ArrayList of type String of distinct values in table and column specified.
	 * <p>
	 * Synchronized on the database connection object.
	 * 
	 * @param	absTablePath		The absolute database.table path
	 * @param	column				The column header for the row returned as distinct
	 * @return	getEncountered			An array of distinct records in column
	 * @see		ArrayList
	 * @see		PreparedStatement
	 * @see		ResultSet
	 */
	public static ArrayList<String> getDistinct(String absTablePath, String column) throws SQLException{
		ArrayList<String> getEncountered = new ArrayList<String>();
		
		synchronized(DB.conn){
			PreparedStatement toEncounter = DB.conn.prepareStatement("SELECT DISTINCT "+column+" FROM "+absTablePath, Statement.RETURN_GENERATED_KEYS);
			ResultSet encountered = toEncounter.executeQuery();
			
			for(int i=1; encountered.absolute(i); i++)
				getEncountered.add(encountered.getString(1));
			
			toEncounter.closeOnCompletion();
		}
		return getEncountered;
	}
}
