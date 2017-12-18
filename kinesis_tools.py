from datetime import datetime
import json
import boto3
import time
import redis


def post_item(client,table,args):
    tb=client.Table(table)
    return tb.put_item(**args)

def update_counter(client,stream,shard,counter):
    table='kinesis_counters'
    args={
        'Item':{
            'stream-shard':stream + '-' + shard,
            'counter':counter
        }
    }
    return post_item(client,table,args)

def kinesis_get_seq(client,stream,shard):
    table = 'kinesis_counters'
    tb=client.Table(table)
    args = {
        'Key':{
            'stream-shard':stream + '-' + shard
        }
    }
    return tb.get_item(**args)['Item']['counter']

def kinesis_get_recs(client,shard_iter_string, rec_count=10):
    resp = client.get_records(
            ShardIterator=shard_iter_string,
            Limit = rec_count
        )
    return resp



def get_shard_iter(client,stream_name,shard):
    if not shard['starting_sequence_number']:
        shard['starting_sequence_number'] = kinesis_get_seq(client,stream_name,shard['name'])
    shard_iter = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard['name'],
            ShardIteratorType='AFTER_SEQUENCE_NUMBER',
            StartingSequenceNumber=shard['starting_sequence_number']
        )
    return shard_iter['ShardIterator']

def redis_update_word_count(redis_client,word_array):
    redis_words = {word:redis_client.get(word) for word in word_array}
    for key,value in redis_words.iteritems():
        try:
            if value:
                value = int(value) + 1 
            else:
                value = 1 
            redis_client.set(key,value)
        except:
            print "failed to set {}".format(key)

class Kinesis():
    def __init__(self,stream_name):
        #get all the high level stream stuff I need
        self.ks = boto3.client('kinesis')
        self.ddb = boto3.resource('dynamodb',region_name='us-west-2')
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
        self.redis = redis.Redis(
            host='localhost',
            port=6379
        )

    def get_starting_seq(self,shard):
        try:
            starting_seq = kinesis_get_seq(self.ddb,self.stream['StreamName'],shard)
        except KeyError:
            starting_seq = None
        return starting_seq

    def set_starting_seqs(self):
        for shard in self.active_shards:
            new_start_seq = self.get_starting_seq(shard['name'])
            if new_start_seq:
                shard['starting_sequence_number'] = new_start_seq

    def get_recs_ongoing(self,shard, rec_count):
        recs = kinesis_get_recs(self.ks,shard['iter'],rec_count)
        if recs['Records']:
            for rec in recs['Records']:
                print rec['Data']
            word_array = [json.loads(rec['Data']).get('field_1') for rec in recs['Records']]
            print word_array
            redis_update_word_count(self.redis,word_array)
            las_rec = recs['Records'][-1]
            last_rec_seq_number = las_rec['SequenceNumber']
            update_counter(self.ddb,self.stream['StreamName'],shard['name'],last_rec_seq_number)
        shard['iter'] = recs['NextShardIterator']
        return shard

    def listen(self,rec_count = 10):
        #need to find a shard and starting position
        while True:
            for shard in self.active_shards:
                if not shard['iter']:
                    shard['iter'] = get_shard_iter(
                        self.ks,
                        self.stream['StreamName'],
                        shard
                    )
                shard = self.get_recs_ongoing(shard,rec_count)
                print shard
                time.sleep(5)

    def post(self,data):
        payload = {'field_1':data}
        payload = json.dumps(payload)
        response = self.ks.put_record(
            StreamName=self.stream['StreamName'],
            Data=payload,
            PartitionKey='data'
        )
        return response