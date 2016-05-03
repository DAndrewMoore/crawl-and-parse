package com.dam_industries.docparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Callable;

import com.dam_industries.dbaccess.*;

public class ParseThreads implements Callable<Boolean> {
	private String doc;
	private ArrayList<String> stopWords;
	private String URL;
	
	//Constructor method since call doesn't take args
	public ParseThreads(String doc, String URL, ArrayList<String> Stops){
		this.doc = doc;
		this.URL = URL;
		this.stopWords = Stops;
	}
	
	//Actual thing that does stuff
	public Boolean call() throws Exception {
		Porter stem = new Porter(); //Create new stemmer object
		
		if(doc != null){ //If we got the text from the page
			
			ArrayList<String> docTokens = new ArrayList<String>(Arrays.asList(UpdaterFuncts.tokenizer(doc))); //Tokenize the text
			doc = null;
			
			//remove stop words
			docTokens.removeAll(stopWords);
			
			//Strip affixes
			ArrayList<String> stemDocTokens = new ArrayList<String>();
			for(String x : docTokens) //for each token in the docTokens
				if(x.length() > 0) //if it's not blank space (not sure why that's getting in here)
					stemDocTokens.add(stem.stripAffixes(x).toLowerCase()); //stem and add to the stemmed doc tokens list
			docTokens = null;
			
			
			//Get term frequencies from tokens
			HashMap<String, Integer> termFreq = new HashMap<String, Integer>();
			for(String x : stemDocTokens){
				int freq = termFreq.getOrDefault(x, 0); //get frequency if it exists, otherwise get 0
				freq++; //update frequency
				termFreq.put(x, freq); //put it in the hashmap
			}
			stemDocTokens = null;
			
			//Push results to database
			Iterator<String> i = termFreq.keySet().iterator();
			while(i.hasNext()){ //for each term encountered
				String term = i.next(); //get term
				
				UpdaterFuncts.updateDocTermFreq(URL, term, termFreq.get(term)); //update table holding URL, term, tf
				
				UpdaterFuncts.updateTermDocCount(term); //update doc count for term
			}
			
			return true;
			
		} else {
			System.out.println("\tWe could not reach URL: "+URL);
		}
		
		return false;
	}

}
