# from kafka import KafkaConsumer
from json import loads
from time import sleep
from kafka import KafkaConsumer
import mysql.connector
'''Program that recieves the data from topic 'portfolios' and inserts them in the databse 'InvestorsDB' '''

# Connect to topic
szer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer('portfolios', bootstrap_servers=['localhost:9999'],
                         auto_offset_reset='latest', enable_auto_commit=True,
                         group_id='InvestorsDB_Insertion', value_deserializer=szer)


for msg in consumer:
    # Connect to Database
    conn = mysql.connector.connect(user='[REDACTED]', password='[REDACTED]', host='[REDACTED]',
                                   database='InvestorsDB')
    cursor = conn.cursor()

    inrt = msg[6]
    pfolio = inrt['port']
    # See which portfolio the data is about
    if pfolio == 'p11':
        table = 'Inv1_P11'
    elif pfolio == 'p12':
        table = 'Inv1_P12'
    elif pfolio == 'p21':
        table = 'Inv2_P21'
    elif pfolio == 'p22':
        table = 'Inv2_P22'
    elif pfolio == 'p31':
        table = 'Inv3_P31'
    elif pfolio == 'p32':
        table = 'Inv3_P32'
    else:
        print('error not valid portfolio...skipping')
        continue

    # Script to insert the new data
    cursor.execute("INSERT INTO {}(C_Date, Eval, Ch_Eval, Ch_Eval_per) ".format(table) + \
                   "VALUES(DATE('{}'), {}, {}, {});".format(inrt['date'], inrt['Eval'], inrt['Change_Evalue'],
                                                 inrt['Change_Evalue_per']))
    # Terminate connection
    conn.commit()
    conn.close()
    sleep(1)