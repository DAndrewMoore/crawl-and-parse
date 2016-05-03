# crawl-and-parse
This program crawls in BFS manner from a statically assigned URL. Some modifications can be made to optimize the transition between crawler and parse, which can be seen forming through cleaning and caching the data post crawl. All information is stored to a MySQL database 'crawler' running on port 3306 on localhost. 'crawler' access is given by default values in DB.java and can be modified before connecting through set commands (see javadocs). 


# src/com/dam_industries
Contains 3 packages:
crawler - the spider, searches in BFS manner from the statically assigned URL and enqueues processes recursively until all href's have been parsed. The crawled sites are pushed to a database table.

dbaccess - controls most access to the database, HelperClass provides methods for the Main to retrieve documents, UpdaterFuncts provides methods for writing to the database as well as caching or cleaning documents.

docparser - Main builds and maintains necessary variables and threads, ParseThreads does the work after being invoked by the Main method, Porter contains tokenizing and stemming functions.

# dependencies
itext-5.5.9 was not written by my self, nor have I altered the code.

mysql-connector-java-5.1.38 was not written by my self, nor have I altered the code.

jsoup was not written by my self, nor have I altered the code.

Porter.java was not written by my self, nor have I altered the code.
