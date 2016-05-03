package com.dam_industries.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;

import org.jsoup.Jsoup;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;


/**
 * UpdaterFuncts are an assortment of functions used to update rows and tables
 * in a predefined database environment. Also there's a tokenizer.
 * <p>
 * Most methods are synchronized so they may not be interrupted while processing
 * and will not overwrite rows by pulling from old data.
 * <p>
 * All database access uses PreparedStatements which are closed immediately after use.
 * <p>
 * If you use this, be sure to update the absolute path to your tables
 * (i.e. database.table).
 * 
 * @author		Andrew Moore
 * @since		1.0
 */
public class UpdaterFuncts {
	
	/**
	 * Updates or creates entry for token specified in parameters.
	 * <p>
	 * Method uses synchronized blocks so we don't lock the entire class on method call.
	 * 
	 * @param	tS	The token to have its entry created or updated
	 * @see		DB
	 */
	public static void updateTermDocCount(String tS) throws SQLException{
		ResultSet meh = null;
		PreparedStatement stmt = null;
		int docuCount = 0;
		
		synchronized(DB.conn){
			stmt = DB.conn.prepareStatement("select * from crawler.tok_table where Token = (?);", Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, tS);
			meh = stmt.executeQuery(); //Result of query on token tS, originally points to space before first row
			
			if(meh.absolute(1)) //Attempt to set the pointer to row 1. If successful, will return value true and pointer will be on row 1.
				docuCount = meh.getInt(2); //Get the document count for term tS
			stmt.close(); //Close connection so we do not have memory leaks
		}
		
		if(docuCount > 0){ //if the row existed, then we'll update that row
			synchronized(DB.conn){
				PreparedStatement updDocCount = DB.conn.prepareStatement("UPDATE crawler.tok_table SET Doc_Count = (?) WHERE Token = (?);", Statement.RETURN_GENERATED_KEYS);
				updDocCount.setInt(1, docuCount);
				updDocCount.setString(2, tS);
				updDocCount.execute();
				updDocCount.closeOnCompletion();
			}
		} else { //If the row did not exist, we'll insert a new row
			synchronized(DB.conn){
				PreparedStatement newTokenInsert = DB.conn.prepareStatement("INSERT INTO crawler.tok_table(Token, Doc_Count) VALUES (?,?);", Statement.RETURN_GENERATED_KEYS);
				newTokenInsert.setString(1, tS);
				newTokenInsert.setInt(2, 1);
				newTokenInsert.execute(); //add row to table
				newTokenInsert.closeOnCompletion();
			}
		}
	}
	
	/**
	 * Updates the merged table. Table is combination of URL->Token->Freq.
	 * <p>
	 * Method specifies synchronized in order to avoid unintentional PreparedStatement
	 * closures.
	 * <p>
	 * Method is synchronized on the database connection and will not allow other threads<br>
	 * to access the database during the update.
	 * 
	 * @param	URL		The URL (Document) that we're adding to
	 * @param	tS		The token we're attaching to the document
	 * @param	freq	The frequency of tS (the token) in the URL (Document)
	 * @see		DB
	 */
	public static void updateDocTermFreq(String URL, String tS, int freq) throws SQLException{
		synchronized(DB.conn){
			PreparedStatement stmt = DB.conn.prepareStatement("INSERT INTO crawler.tok_freq(Doc_ID, Token, Freq) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, URL);
			stmt.setString(2, tS);
			stmt.setInt(3, freq);
			stmt.execute();
			stmt.closeOnCompletion();
		}
	}
	
	/**
	 * Deletes the URL specified from the table of records we don't need to parse.
	 * <p>
	 * A good chunk of URLs parsed lead to pictures, pdf files, etc. 
	 * There's no reasonable way to handle them, so 
	 * just throw them out when we get the chance.
	 * <p>
	 * Method specifies synchronized in order to avoid unintentional PreparedStatement
	 * closures.
	 * 
	 * @param	URL	The URL we wish to delete from our scraping record table
	 * @see		DB
	 */
	public static void trashURL(String URL) throws SQLException{
		synchronized(DB.conn){
			PreparedStatement stmt = DB.conn.prepareStatement("DELETE FROM crawler.record WHERE URL = (?);");
			stmt.setString(1, URL);
			stmt.execute();
			stmt.closeOnCompletion();
		}
	}
	
	/**
	 * Removes inaccessible records from the records table.
	 * <p>
	 * Does not document why they are not accessible, will use {@link UpdaterFuncts#trashURL(String)} to <br>
	 * remove the records.
	 * <p>
	 * Prints progress every 500 iterations.
	 * <p>
	 * Usees {@link UpdaterFuncts#updQuickDocs} to locally store parsed documents for future use.
	 * 
	 * @param records 
	 * @param db
	 * @throws SQLException
	 */
	public static void cleanRecords(ArrayList<String> records, DB db) throws SQLException{
		int stored = 0;
		int removed = 0;
		boolean weFailedDueToError = false;
		
		for(int i=0; i<records.size(); i++){
			if(i % 500 == 0)
				System.out.println(LocalTime.now()+" - At index "+i+" cached "+stored+" removed "+removed);
			
			String URL = records.get(i);
			try{
				String doc = Jsoup.connect(URL).get().text();
				updQuickDocs(URL, doc);
				URL = null;
				doc = null;
				stored++;
			} catch (MySQLSyntaxErrorException e) {
				System.out.println("Shits broke yo");
			} catch (OutOfMemoryError e){
				System.out.println("We're still having memory leaks");
				weFailedDueToError = true;
				break;
			} catch (Exception e){
				trashURL(URL);
				removed++;
			}
		}
		
		if(!weFailedDueToError)
			System.out.println("Succesfully cleaned and cached the records");
		else
			System.out.println("We failed due to a heap overflow");
	}
	
	
	public static void updQuickDocs(String URL, String doc) throws SQLException{
		synchronized(DB.conn){
			PreparedStatement ps = DB.conn.prepareStatement("INSERT INTO `quick_docs`(`URL`, `Document`) VALUES (?,?);", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, URL);
			ps.setString(2, doc);
			ps.execute();
			ps.closeOnCompletion();
		}
	}
	
	/**
	 * Tokenizes the string (document)
	 * <p>
	 * The string will be tokenized in the following order:<br>
	 * 	1. Remove apostrophe<br>
	 * 	2. Remove HTML tags (Anything between < and >)<br>
	 * 	3. Convert all punctuation not already removed to "\s"<br>
	 * 	4. Split on whitespace+ and newline+<br>
	 * 
	 * @param	doc		The string of text to be tokenized
	 * @return	tDoc	A static string array of tokens (warning, this may return spots with '\0' be sure to check str.length > 0)
	 */
	public static String[] tokenizer(String doc){ //Tokenization method for text
		doc = doc.replaceAll("\'",""); //remove '
		doc = doc.replaceAll("<[^>]*", ""); //remove HTML tags (this has actually been taken care of by JSOUP)
		doc = doc.replaceAll("\\p{Punct}", " "); //expand by \s on any punctuation
		String[] tDoc = doc.split("[\\s+\\n+]"); //split by new line or white space
		return tDoc; //return individual tokens
	}
}
