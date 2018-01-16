from datetime import datetime
import json
import boto3
import time


def sample_batch_processing_function(list_of_records):
    for i in list_of_records:
        i=i.strip()
        i=json.loads(i)
        print i['f1']

class KinesisConsumer():
    def __init__(self,stream_name,consumer_name,ddb_table = 'kinesis_counters'):
        self.ks = boto3.client('kinesis')
        self.ddb = boto3.resource('dynamodb',region_name='us-west-2')
        self.consumer_name = consumer_name
        self.ddb_table = ddb_table
        self.stream = self.ks.describe_stream(
            StreamName=stream_name,
        )['StreamDescription']
        self.active_shards = [
            {
                'name':s['ShardId'],
                'starting_sequence_number':s['SequenceNumberRange'].get('StartingSequenceNumber'),
                'iter':None
            } for s in self.stream['Shards'] 
            if not s['SequenceNumberRange'].get('EndingSequenceNumber')
        ]

    def get_active_sequence_number(self,shard_name):
        """return a dictionary of the shards and their last successfully processed sequence numbers
        If there is no entry for last successfully processed record, return none, and handle downstream"""
        tb=self.ddb.Table(self.ddb_table)
        args = {
            'Key':{
                'stream-shard':self.stream['StreamName'] + '-' + shard_name + ':' + self.consumer_name
            }
        }
        r = tb.get_item(**args)
        if not r.get('Item'):
            return None
        else:
            return r['Item'].get('counter')

    def get_shard_iter(self,shard_name, position = None, latest = False, first = False):
        """return a shard iterator starting at position
        if the position and latest are false, then start at the trim horizon"""
        args = {
            'StreamName':self.stream['StreamName'],
            'ShardId': shard_name
        }
        if position:
            args['ShardIteratorType'] = 'AFTER_SEQUENCE_NUMBER'
            args['StartingSequenceNumber'] = position
        elif latest:
            args['ShardIteratorType'] = 'LATEST'
        elif first:
            args['ShardIteratorType'] = 'TRIM_HORIZON'
        else:
            raise ValueError("must specify either position, latest or first. \
                Please edit the map passed to get_shard_position() to include a \
                starting position for shard {}".format(shard_name))
        return self.ks.get_shard_iterator(**args)['ShardIterator']

    def set_shard_position(self,seq_num_map = False, DDB=True, first = False, latest = True):
        """set the starting position for each shard
        logic is:
            seq_num_map
            DDB seq number (if true)
            trim horizon
        The idea is that if I want to track one record through the stream then 
        I can feed the sequence number(s) into this function. Otherwise the function
        will default to start after the latest Seq num for each shard as saved in DDB.
        If there is no DDB entry for a shard then it will default to the trim horizon"""
        for shard in self.active_shards:
            print "getting shard iterator for shar {}".format(shard['name'])
            if seq_num_map:
                if seq_num_map.get(shard['name']):
                    shard['iter'] = self.get_shard_iter(shard['name'],position = seq_num_map[shard['name']])
            elif DDB:
                pos = get_active_sequence_number(shard['name'])
                if pos:
                    shard['iter'] = self.get_shard_iter(shard['name'],position = pos)
            elif first:
                shard['iter'] = self.get_shard_iter(shard['name'],first = True)
            else:
                shard['iter'] = self.get_shard_iter(shard['name'], latest = True)

    def reset_ddb_counter(self):
        """use this function to erase the history of processing from DDB"""
        pass 

    def poll(self,shard,num_recs):
        """run get_records on the stream and then process the records returned with the 
        processing_function"""
        resp = self.ks.get_records(
            ShardIterator=shard['iter'],
            Limit = num_recs
        )
        return resp

    def listen(self,delay_secs=5,recs_process=1000,print_level=None,processing_function=None,batch_processing_function=None):
        """poll the stream every delay_secs seconds. Throughput:
            5 reads per second per shard and max total read of 2MB per second
            1MB per shard per second and 1000 records per shard per second
        Throw an exception when read progress slows down. if milliseconds_behind_latest is greater than 1 hour
        this function will send an alert 
        Apply the processing function to the records recieved inline
        Apply the batch processing function to the accumulated records at the end of the loop"""

        results = {'recs':[]}
        failed_records = []
        while True:
            for shard in self.active_shards:
                resp = self.poll(shard,recs_process)
                if print_level == 'full':
                    print resp
                #results['ms_bh_latest'],results['records'],results['shard_iter'] = 
                if resp.get('Records'):
                    if print_level == 'data':
                        for r in resp['Records']:
                            print r['Data']
                    if print_level == 'count':
                        print len(resp['Records'])
                    results['recs'] += [ r['Data'] for r in resp['Records']]
                shard['iter'] = resp['NextShardIterator']
            if resp['MillisBehindLatest'] > 0:
                print "ms behind latest: {}".format(resp['MillisBehindLatest'])
                time.sleep(delay_secs)
            else:
                break
        if results['recs']:
            if batch_processing_function:
                batch_processing_function(results['recs'])

    def post(self,data,partition_key='1'):
        payload = data
        payload = json.dumps(payload) + '\n'
        response = self.ks.put_record(
            StreamName=self.stream['StreamName'],
            Data=payload,
            PartitionKey=partition_key
        )
        return response

# k = KinesisConsumer('my-first-stream','c1')
# k.set_shard_position(DDB=False,latest=True)
# k.listen()
