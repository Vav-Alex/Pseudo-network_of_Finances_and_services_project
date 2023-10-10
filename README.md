# Pseudo-network_of_Finances_and_services_project
Term project for the course Big Data Architechture. Applications proccesing messages from pseudo-servers and sending results a database and an application proccesing the historical data of the database to create some statistical insights.

# Description
This project is a setup of a network of financial and investment services of stock exchanges and investors as well as applications that analyse those data. This network can run in Hadoop and the components of the network are described below.

Note: Comments and descriptions are provided in all the python files for better understanding.

Two Stock Exchange servers. Each server emits 12 stocks and their price as JSON objects to Kafka at the close of each day. Stock Exchanges trade every day except weekends and Greek holidays. The server simulates the functions of a fictious Stock Exchange and emits JSON objects to a port at the closing of each day. The server emits the JSON objects to a Kafka topic “StockExchange” for every day since 1-1-2000, except weekends and Greek national holidays. The duration of each day will be simulated with at least 2 seconds of sleep, i.e., the server will emit no earlier than every 2 seconds the closing stock prices for a date. One server emits the first 12 stocks while the other one emits the rest.
These are the stocks and their initial prices for both servers:
 
 

Solution Approach:
The files se1_server.py and se2_server.py represent those servers. These files will continually run and change the stock prices throughout the day and emit them at the end of the day in 12 messages each (24 total). Each message contains the name of the stock, its new price and the date of the day. The new prices are constructed using the previous ones multiplied by a random multiplier. For 01-01-2000 the initial prices are emitted and then enters a continuous loop that every 2 seconds sends the new prices before changing the date except for weekends and Greek holidays. The Kafka topic ‘StockExchange’ departs of 2 partitions (each server sends to only one of those partitions), so that in a scenario where an investor is interested in the stocks of only one of the servers, his application will not receive those of the other one.
 

Three Institutional Investors Inv1, Inv2, and Inv3. Each investor manages two portfolios as follows: Inv1 manages P11 and P12, Inv2 manages P21 and P22, and Inv3 manages P31 and P32. The portfolios are structured as follows:
 
Each investor file reads from the Kafka topic “StockExchange” and evaluates each of its portfolios for each day. The results are then writen to a Kafka topic ‘portfolios’ a JSON object that contains:
a. the evaluation of the portfolio for each day
b. difference from the previous day’s evaluation
c. percentage difference from previous day’s evaluation, as well as any other necessary information. The investors should be implemented in files inv1.py, inv2.py, inv3.py, respectively.

Solution Approach:
Each file reads from the topic “StockExchange” to obtain the information regarding the new prices. Specifically, they read from the partitions that regard the stocks each investor’s portfolios concern (in this scenario all files read from both partitions). They continuously read messages from the topic till they have acquired the days prices for all the stocks regarding the investor and then create an evaluation based on the prices of the stock and the quantities of them in the portfolio. After the evaluations are created, the application sends a message in JSON form containing the wanted results in the topic “portfolios”. Then the application resets and moves to the next day.
 

An application, which creates a MySQL database ‘InvestorsDB’ with the following tables
1. ‘Investors’ with fields ‘Id’, ‘Name’, ‘City’. 
2. ‘Portfolios’ with fields ‘Id, ‘Name’, ‘Cumulative’. 
3. ‘Investors_Portfolios’ with fields ‘iid’, ‘pid’ that connects investors and portfolios.
4. A table for each investor’s portfolio, e.g., Inv1_P11, Inv12_P12, and so on. The content of each table must be the evaluation of the date as well as the difference both in quantity and percentage of it from the previous day.

Solution Approach:
In the file ‘InvestorsDB’, using the library MYSQL, an SQL script creating the tables as instructed is executed. Additionally, the table ‘Investors_Portfolios’ is used as a connecting table between the tables ‘Investors’ and ‘Portfolios’.

A Spark DF application that queries the ‘InvestorsDB’ database and for a given investor produces a file for each of its portfolios with name __stats that contains the following. 
•	The maximum and minimum daily change and percentage change for the full history of the portfolio.
•	The maximum and minimum daily change and percentage change for the full history of the portfolio per year.
•	The average evaluation and the standard deviation of evaluation for the full history of the portfolio.
•	The average evaluation and standard deviation of the portfolio for a given period, e.g., 2018 to 2021.
•	The average evaluation per month for the full history of the portfolio, with the most recent month listed first.

Solution Approach:
For this part of the project, the function ‘fetch_story’ takes an investor as input as well as a starting year and end year which are needed for the 4th question. After connecting to the database, it obtains the table containing all historical data of the investors portfolios.  Then it passes it, as well as premade file names, to another function called port_work, which uses the functions from SparkSession library to neatly create the wanted results. Finally, the application writes the results in text files, each regarding one of the portfolios.

# Execution

INITIALIZAION
Having both kafka and zookeper installed and in an active status:
create the topics 'StockExchange' with 2 partitions and 'portfolios' with 1 using the following commands:
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic StockExchange

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic portfolios

Additionaly, in app2.py on line 28, make sure that 'mysql-connector-java-8.0.28.jar' is in the directory mentioned in the line,
	for sometimes we changed it for our convinience. However upon delivery the file will propably not require any change

Run file InvestorsDB for the creation of the database.

RUNNING'

Excecute the following instructions in order:

1) Run se1_server.py and se2_server.py to generate data to the topic 'StockExchange'

2) Run inv1.py, inv2.py, inv3.py to consume messages from the topic 'StcokExchange' and send portfolios evaluations 
	to topic 'portfolios'
3) Finally Run app1.py to consume messages from topic 'portfolios' and store them in the database InvestorsDB

AGGREGATIONS

in order to find out statistical information for the portfolios of a given investor.
Go to app2.py at line 102 and enter the number of the investor you are interasted in (1,2 or 3)
	and the period of time you wish to have some of the statistical results based one 
	note that the year in the second position must be before from the on in third position 
	E.G. (1, '2013', '2017')
