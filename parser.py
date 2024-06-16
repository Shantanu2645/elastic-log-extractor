import csv
from datetime import datetime
import pytz
from elasticsearch import Elasticsearch

def convert_to_bdt_with_ms(timestamp_utc_str):
    timestamp_utc = datetime.fromisoformat(timestamp_utc_str[:-1]) if timestamp_utc_str else None
    if timestamp_utc:
        # Define the UTC timezone
        utc_timezone = pytz.utc
        # Localize the datetime object to UTC timezone
        timestamp_dt_utc = utc_timezone.localize(timestamp_utc)
        # Define the Bangladesh Time (BDT) timezone
        bdt_timezone = pytz.timezone('Asia/Dhaka')
        # Convert the datetime object from UTC to BDT timezone
        timestamp_dt_bdt = timestamp_dt_utc.astimezone(bdt_timezone)
        # Format the datetime object as a string in BDT format with milliseconds
        return timestamp_dt_bdt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return ''


ELASTIC_PASSWORD = "<elastic_search_password>"

#cert fingerprint is mandatory. Input a wrong cert fingerprint. It will show the original one in the error message.
#Now the real fingerprint can be formatted like the below format and use.
CERT_FINGERPRINT = "21:6F:9A:15:9D:85:1C:B2:0D:F2:2F:CE:BE:5F:EF:06:75:0C:19:4B:3A:3A:9A:77:3B:06:B0:3C:81:8B:A0:52"

# Create the Elasticsearch client instance
es = Elasticsearch(
    "https://<elasticsearch_address>",
    ssl_assert_fingerprint=CERT_FINGERPRINT,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

START_DATE_UTC = datetime(2024, 2, 18, 18, 0, 0, tzinfo=pytz.utc)
END_DATE_UTC = datetime(2024, 2, 24, 17, 59, 59, 999999, tzinfo=pytz.utc)

START_DATE_STR = START_DATE_UTC.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
END_DATE_STR = END_DATE_UTC.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

DATE_PREFIX = "2024-02-18-24"


#change the query as you need
query = {
    "query": {
        "bool": {
            "filter": [
                {"range": {"@timestamp": {"gte": START_DATE_STR, "lte": END_DATE_STR}}},
                {"term": {"consumer.username.keyword": "xxxxxxxxxx"}},
                {"term": {"response.status": 200}},
                {"term": {"request.uri.keyword": "/api/xxxxxxxxxxxxx/nid/details-v1"}}
            ]
        }
    },
     "size": 10000 
}

#Change index name according to yours
result = es.search(index="kong-*", body=query, scroll="30m")

# Extract the initial scroll ID and hits from the Elasticsearch response
scroll_id = result['_scroll_id']
hits = result['hits']['hits']

# Define the field names for the CSV file
fieldnames = ['@timestamp', 'consumer.username', 'latencies.kong', 'latencies.proxy',
                'latencies.request', 'request.headers.content-length', 'request.headers.content-type',
                'request.headers.host', 'request.headers.x-forwarded-for', 'request.headers.x-forwarded-proto',
                'request.headers.x-real-ip', 'request.method', 'request.size', 'request.uri', 'response.headers.date', 'response.headers.x-kong-upstream-latency',
                'response.size', 'response.status', 'route.created_at']

# Specify the CSV file path
csv_file_path = f'elasticsearch_results_{DATE_PREFIX}.csv'

# Open the CSV file in write mode
with open(csv_file_path, 'w', newline='') as csvfile:

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    while hits:
        for hit in hits:
            source = hit['_source']
            row = {
                '@timestamp': convert_to_bdt_with_ms(source.get('@timestamp', '')),
                'consumer.username': source.get('consumer', {}).get('username', ''),
                'latencies.kong': source.get('latencies', {}).get('kong', ''),
                'latencies.proxy': source.get('latencies', {}).get('proxy', ''),
                'latencies.request': source.get('latencies', {}).get('request', ''),
                'request.headers.content-length': source.get('request', {}).get('headers', {}).get('content-length', ''),
                'request.headers.content-type': source.get('request', {}).get('headers', {}).get('content-type', ''),
                'request.headers.host': source.get('request', {}).get('headers', {}).get('host', ''),
                'request.headers.x-forwarded-for': source.get('request', {}).get('headers', {}).get('x-forwarded-for', ''),
                'request.headers.x-forwarded-proto': source.get('request', {}).get('headers', {}).get('x-forwarded-proto', ''),
                'request.headers.x-real-ip': source.get('request', {}).get('headers', {}).get('x-real-ip', ''),
                'request.method': source.get('request', {}).get('method', ''),
                'request.size': source.get('request', {}).get('size', ''),
                'request.uri': source.get('request', {}).get('uri', ''),
                'response.headers.date': source.get('response', {}).get('headers', {}).get('date', ''),
                'response.headers.x-kong-upstream-latency': source.get('response', {}).get('headers', {}).get('x-kong-upstream-latency', ''),
                'response.size': source.get('response', {}).get('size', ''),
                'response.status': source.get('response', {}).get('status', ''),
                'route.created_at': source.get('route', {}).get('created_at', '')
            }
            writer.writerow(row)
        
        # Scroll to retrieve the next batch of results
        result = es.scroll(scroll_id=scroll_id, scroll='30m')
        
        scroll_id = result['_scroll_id']
        hits = result['hits']['hits']

print(f"Results have been saved to {csv_file_path}")