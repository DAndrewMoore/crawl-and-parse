package com.dam_industries.crawler;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.dam_industries.dbaccess.*;

public class Spider {
		//Recursive method to crawl webpages in BFS manner
		public static void processPage(String URL) throws IOException, SQLException {
			//check if the given URL is already in the database
			//originally done without setting the string which may lead to terrible horrible things (Poor little Bobby Tables)
			PreparedStatement tempStatement = DB.conn.prepareStatement("select * from Record where URL = (?);", Statement.RETURN_GENERATED_KEYS);
			tempStatement.setString(1, URL);
			ResultSet rs = tempStatement.executeQuery();
			
			if(rs.next()){rs.close(); tempStatement.close();} 
			else if (!URL.toLowerCase().contains("unt.edu")) {rs.close(); tempStatement.close();} 
			else if (URL.contains("mailto")) {rs.close(); tempStatement.close();}
			else{
				rs.close();
				tempStatement.close();
				//store the URL to database to avoid parsing again
				PreparedStatement stmt = DB.conn.prepareStatement("INSERT INTO crawler.record(URL) VALUES (?);", Statement.RETURN_GENERATED_KEYS);
				stmt.setString(1, URL);
				stmt.execute();
				stmt.closeOnCompletion();
				
				//get useful information
				try{
					Document doc = Jsoup.connect(URL).get();
					
					//get all links and recursively call the processPage method
					Elements questions = doc.select("a[href]"); //get links from page
					for(Element link: questions) //for each link
						processPage(link.attr("abs:href")); //recursively call with the absolute reference to that linked webpage
				
				} catch(Exception e) {
					System.out.println("K");
				}	
			}
		}
}
