import json
import random
import datetime
import time
import holidays
from kafka import KafkaProducer
'''
Represents the second pseudo-server, made for emmiting the stock prices of the remaining 12 stocks.
Initially, it emmits the starting price while afterwards emmits new prices every day,
with the exception of weekends and Greek holidays
until it reaches todays date. Each day is simulated as 2 secconds.
The data is sent to a different partition(in comparision to the first psudo-server)
of the Kafka topic 'StockExchange' in json form and
each message/file contains the date, the name of the stock and the new price. 
'''
# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9999'])

# Create staring prices
stocks = {
    'HPQ': 27.66, 'CSCO': 48.91, 'ZM': 69.65, 'QCOM': 119.19,
    'ADBE': 344.80, 'VZ': 37.91, 'TXN': 172.06, 'CRM': 182.32,
    'AVGO': 625.15, 'NVDA': 232.88, 'VMW': 120.05, 'EBAY': 43.98
}

# Set the start and end dates
start_date = datetime.date(2000, 1, 1)
end_date = datetime.date.today()

# Get the list of Greek holidays
gr_holidays = holidays.GR()

# Send starting prices
for ticker in stocks.keys():
    msg_1st = {
        "date": str(start_date),
        "stock": ticker,
        "price": stocks[ticker]
    }
    producer.send("StockExchange", json.dumps(msg_1st).encode(), partition=1)
    time.sleep(0.1)
# Move to next day
current_date = datetime.timedelta(days=1) + start_date

# 'start' value and 'currently' value are used to change the day when 2 seconds have passed
start = time.perf_counter()
while current_date <= end_date:

    # New price: Random ticker X Random multiplier
    rs = random.randint(1, len(stocks)) - 1
    r = random.random() / 10 - 0.05

    ticker = list(stocks.keys())[rs]
    stocks[ticker] = round((1 + r)*stocks[ticker], 3) # update the price on the server
    time.sleep(0.1)
    currently = time.perf_counter()

    # Check if day should change
    if currently - start >= 2:
        # If so, send the days resulting prices to the Kafka topic
        for ticker in stocks.keys():
            msg = {
                "date": str(current_date),
                "stock": ticker,
                "price": stocks[ticker]
            }
            # Send resulting tickets and prices for the day (12 messages in total)
            producer.send("StockExchange", json.dumps(msg).encode(), partition=1)

        # Next day
        current_date += datetime.timedelta(days=1)
        # Skip holidays and weekends but sleep 2 seconds for each day
        while current_date.weekday() >= 5 or current_date in gr_holidays:
            time.sleep(2)
            current_date += datetime.timedelta(days=1)
            
        start = time.perf_counter() # Start of new day