from kinesis_consumer import *
from time import sleep
import sys

k = KinesisConsumer('my-first-stream','producer')
dictify = lambda x:{'a':x}


i=0
batch = 0
recs = []
for line in sys.stdin:
	recs.append(dictify(line))
	i += 1
	if i == 25:
		batch += 1
		print "posting batch {}".format(batch)
		i = 0
		k.batch_post(recs)
		recs = []
		sleep(5)

#post the last batch
k.batch_post(recs)