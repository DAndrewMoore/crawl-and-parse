package com.dam_industries.docparser;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.dam_industries.dbaccess.*;

public class Main extends Thread {	
	
	//Only used once during original database set-up
	public static void loadStopWords() throws FileNotFoundException, SQLException{
		Scanner blah = new Scanner(new File("C:\\Users\\Andrew\\Desktop\\4930.002\\stopwords.txt"));
		
		while(blah.hasNext()){
			String stopWord = blah.next();
			String sql = "INSERT INTO crawler.stop_words(nope) VALUES (?)";
			PreparedStatement stmt = DB.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, stopWord);
			stmt.execute();
			stmt.closeOnCompletion();
		}
		blah.close();
	}
	
	//Solution to heap space issue, essentially kill off processes and pheonix the threadpool
	public static void waitForInvoke(Collection<ParseThreads> pool, ExecutorService tPool) throws SQLException{
		System.out.println(LocalTime.now()+" - Starting invoke");
		try {
			tPool.invokeAll(pool); //invoke all in the pool
			tPool.shutdown();  //shutdown the threadpool
			tPool.awaitTermination(30, TimeUnit.MINUTES); //but not for at least 30 minutes
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(LocalTime.now()+" - Finished invoke");
	}
	
	public static ArrayList<String> getTheRest() throws SQLException{
		//Gets all of the websites regardless of if they've been crawled
		ArrayList<String> records = HelperClass.getRecords("crawler.quick_docs", "URL");
		System.out.println(LocalTime.now()+" - Loaded all records with size "+records.size());
		
		//load previously encountered URLs
		ArrayList<String> encountered = HelperClass.getDistinct("crawler.tok_freq", "Doc_ID");
		System.out.println(LocalTime.now()+" - Loaded previously scraped pages with size "+encountered.size());
		
		//remove all the previously encountered webpages from pages to parse
		records.removeAll(encountered);
		System.out.println(LocalTime.now()+" - Removed previously seen with new size "+records.size());
		
		return records;
	}
	
	//Parse for webpages meant to be scraped, takes in webpages that have already been crawled
	public static void parseDocs() throws SQLException, IOException, InterruptedException{
		//Included a helper class to tidy code
		
		//Get the rest of the documents we need to parse
		ArrayList<String> records = getTheRest();
		
		//load stop words into local memory
		ArrayList<String> stopWordList = HelperClass.getRecords("crawler.stop_words", "nope");
		System.out.println(LocalTime.now() + " - Stop words loaded");
		
		//Create collection of callables and pool of executors
		Collection<ParseThreads> pool = new ArrayList<ParseThreads>(); //create new arrayList of callable tasks
		ExecutorService tPool = Executors.newFixedThreadPool(50); //create fixed thread pool
		
		System.out.println(LocalTime.now() + "- Beginning thread creation");
		for(int i=0; i<records.size(); i++){ //loop through sites to parse
			
			//get the cached document
			PreparedStatement tPS = DB.conn.prepareStatement("SELECT Document FROM crawler.quick_docs WHERE URL = (?);", Statement.RETURN_GENERATED_KEYS);
			tPS.setString(1, records.get(i));
			ResultSet rs = tPS.executeQuery();
			
			if(rs.absolute(1)){ //leftover if statement
				ParseThreads cd = new ParseThreads(rs.getString(1),records.get(i), stopWordList); //Create the callable task
				pool.add(cd); //add to our list of callable objects
				
				//Trash collection
				cd = null;
			}
			
			rs.close(); //close unnecessary objects
			tPS.close();
			
			if(pool.size() == 50){ //if we've hit our pool limit
				waitForInvoke(pool, tPool); //wait for all 50 tasks to be completed
				pool = new ArrayList<ParseThreads>(); //Rebirth the callable array
				tPool = Executors.newFixedThreadPool(50); //Rebirth the thread pool
			}
			
			//automatic garbage collection flag if we need it
			if(i%200 == 0)
				System.gc();
		}
		System.out.println("Home stretch");
		waitForInvoke(pool, tPool);
	}
	
	public static void main(String args[]) throws IOException, SQLException, InterruptedException {
		DB db = new DB(); //Spawn the database object
		db.connectMe(); //Connect to the database
		
		
		System.out.println("Parse Docs");
		parseDocs();
		
	}
	
	/* Scratch Space
	 	//Restart from scratch
		//db.runSql2("TRUNCATE record");
		//processPage(db, "http://www.unt.edu/");
		
		//If we need to reload stop words
		//db.runSql2("TRUNCATE stop_words");
		//System.out.println("Load stop words");
		//loadStopWords(db);
		
		//Restart prasing
		//db.runSql2("TRUNCATE tok_table");
		//db.runSql2("TRUNCATE tok_freq");
	 */
}