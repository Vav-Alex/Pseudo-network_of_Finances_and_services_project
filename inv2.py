from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from time import sleep
import json
'''Files inv1, inv2 and inv3 are programms which recieve data from both partitions of the Topic 'StockExchange'.
They constantly recieve the data until they have acquired prices for all the stocks of the portfolios of the corresponding investor.
Once prices for all stocks are recieved, it is assumed they are the final prices for the day, which is when the evaluation of
the portfolio is created and sent to a different Kafka topic, 'portfolios'.
The data sent to the topic consist of the date, the name of the portfolio, its evaluation, and its difference from
the previouses day evaluation in quantity and percentage
'''

# Connect with 2 Consumers to Kafka topic "StockExchange"
vdszer = lambda x:json.loads(x.decode('utf8'))
consumer1 = KafkaConsumer(bootstrap_servers=['localhost:9999'],
                         auto_offset_reset='latest',
                         enable_auto_commit = False,
                         group_id='Inv2',
                         value_deserializer=vdszer)

consumer1.assign([TopicPartition('StockExchange', 0)])

consumer2 = KafkaConsumer(bootstrap_servers=['localhost:9999'],
                         auto_offset_reset='latest',
                         enable_auto_commit = False,
                         group_id='Inv2',
                         value_deserializer=vdszer)

consumer2.assign([TopicPartition('StockExchange', 1)])



# Connect to Kafka topic "portfolios"
producer = KafkaProducer(bootstrap_servers=['localhost:9999'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Define the initial values of the previous day's evaluation for each stock in the portfolio
stock_prices = {"HPQ": 0, "CSCO": 0, "ZM": 0, "QCOM": 0, "ADBE": 0, "VZ": 0,
                "TXN": 0, "CRM": 0, "AVGO": 0, "NVDA": 0, "VMW": 0, "EBAY": 0}


stock_acquired = dict.fromkeys(stock_prices, False)


today = '2000-01-01'

while True:
    data_se1 = next(consumer1)[6]
    data_se2 = next(consumer2)[6]
    sleep(0.1)
    
    # If we are interested in the stock we take it
    if data_se1['stock'] in list(stock_prices.keys()):
        stock_prices[data_se1['stock']] = data_se1['price']
        stock_acquired[data_se1['stock']] = True

    if data_se2['stock'] in list(stock_prices.keys()):
        stock_prices[data_se2['stock']] = data_se2['price']
        stock_acquired[data_se2['stock']] = True
        
    # Check if all stock prices have been updated, if so do evaluation
    if (all(stock_acquired.values())):

        # Calculations
        current_eval_p1 = {"HPQ": stock_prices["HPQ"] * 1600, "CSCO": stock_prices["CSCO"] * 1700,
                           "ZM": stock_prices["ZM"] * 1900,
                           "QCOM": stock_prices["QCOM"] * 2100, "ADBE": stock_prices["ADBE"] * 2800,
                           "VZ": stock_prices["VZ"] * 1700}
        current_eval_p2 = {"TXN": stock_prices["TXN"] * 1400, "CRM": stock_prices["CRM"] * 2600,
                           "AVGO": stock_prices["AVGO"] * 1700,
                           "NVDA": stock_prices["NVDA"] * 1800, "VMW": stock_prices["VMW"] * 2600,
                           "EBAY": stock_prices["EBAY"] * 1800}

        current_eval_p1_sum = sum(current_eval_p1.values())
        current_eval_p2_sum = sum(current_eval_p2.values())
        
        # Only for the 1st day since we don't have data for the day before
        if today == '2000-01-01':
            prev_eval_p1 = current_eval_p1_sum
            prev_eval_p2 = current_eval_p2_sum

        # Calculate the difference from the previous day's evaluation for each portfolio
        eval_diff_p1 = current_eval_p1_sum - prev_eval_p1
        eval_diff_p2 = current_eval_p2_sum - prev_eval_p2


        # Calculate the difference percentage
        try:
            percentage_diff_p1 = (eval_diff_p1 / prev_eval_p1)
        except:
            percentage_diff_p1 = 0
            
        try:
            percentage_diff_p2 = (eval_diff_p2 / prev_eval_p2)
        except:
            percentage_diff_p2 = 0


        # Build the JSON object to send to Kafka topic "portfolios"
        p1_data = {"date": today,
                   "port": "p21",
                   "Eval": current_eval_p1_sum,
                   "Change_Evalue": eval_diff_p1,
                   "Change_Evalue_per": percentage_diff_p1
                   }

        p2_data = {"date": today,
                   "port": "p22",
                   "Eval": current_eval_p2_sum,
                   "Change_Evalue": eval_diff_p2,
                   "Change_Evalue_per": percentage_diff_p2
                   }

        producer.send('portfolios', p1_data)
        producer.send('portfolios', p2_data)

        # Update the previous evaluation values
        prev_eval_p1 = current_eval_p1_sum
        prev_eval_p2 = current_eval_p2_sum

        # Prepare for next day
        today = data_se1['date']
        stock_acquired = dict.fromkeys(stock_acquired, False)