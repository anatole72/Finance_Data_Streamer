import json
import os
import subprocess
import sys
import boto3
import datetime

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf

def stream(stock):
    #Create client
    fh = boto3.client("firehose","us-east-1")

    for i in stock.index:
        record = stock.loc[i].to_json()
    
        #put record to firehose stream
        fh.put_record(
            DeliveryStreamName="stock-price-stream", 
            Record={"Data": record.encode('utf-8')})
    
def lambda_handler(event, context):
    begin_time = datetime.datetime.now()
    
    symbols =["FB","SHOP","BYND","NFLX","PINS","SQ","TTD","OKTA","DDOG","SNAP"]
    record_count=0
    
    #Download stock price data
    for symbol in symbols:
        tmp = yf.download(tickers =symbol, interval="1m", start="2020-05-14",end="2020-05-15")
        tmp.reset_index(inplace=True)
        
        tmp['name'] = symbol
        tmp['ts'] =tmp['Datetime'].astype(str)
        tmp.columns = map(str.lower, tmp.columns)
        
        tmp = tmp[['high','low','ts','name']]
        #send prices to stream function
        stream(tmp)
        record_count+=len(tmp)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Complete! took {datetime.datetime.now()-begin_time} and loaded {record_count} json records')
    }

