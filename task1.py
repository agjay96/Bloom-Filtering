from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import time
import binascii
import datetime
import sys
import random

filter_array = [0]*69997

present = dict()

fp = 0
tn = 0

port = int(sys.argv[1])
filename = sys.argv[2]


def myhashs(x):
	ans=[]
	a=[]
	b=[]
	for i in range(20):
		k=random.randint(0,1516189)
		while k in a:
			k=random.randint(0,1516189)
		a.append(k)
		while k in a:
			k=random.randint(0,1516189)
		b.append(k)
		# a=random.randint(0,1516189)
		# b=random.randint(0,1516189)
		ans.append(((a[-1]*x + b[-1])%69997))

	return ans

		#print("a", a, "b", b)
		# (10037*x + 20357) % 69997,
		# (43971*x + 9873) % 69997,
		# (30687*x + 53579) % 69997,
		# (8193*x + 57041) % 69997,
		# (62087*x + 347) % 69997

	# return [
	# 	(433*x + 173) % 69997,
	# 	(2691*x + 3117) % 69997,
	# 	(6194*x + 6173) % 69997,
	# 	(1733*x + 1791) % 69997,
	# 	(52337*x + 57119) % 69997,
	# 	(69875*x + 519) % 69997,
	# 	(547*x + 7119) % 69997
	# 		]

	


def hash(x, city):

	global filter_array, present

	c = []

	for i in myhashs(x):
		c.append(filter_array[i])

	if all(c) == 1:
		try:
			if present[city] == 1:
				return True, 0
		except KeyError:
			return False, 1

	else:
		for i in myhashs(x):
			filter_array[i] = 1
		present[city] = 1
		return True, 1


def test(x):

	f = open(filename, "a+")

	global fp
	global tn

	x = x.collect()
	for i in x:
		# business = json.loads(i)
		truth_value, count = hash(int(binascii.hexlify(i.encode('utf-8')), 16), i)
		if truth_value:
			tn += count
		else:
			fp += count

	fpr = 0

	try:
		fpr = fp/(fp+tn)
	except ZeroDivisionError:
		pass

	time.sleep(33)
	f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(fpr) + "\n")

	f.close()


if __name__ == "__main__":

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	scc = StreamingContext(sc, 10)
	streaming_c = scc.socketTextStream("localhost", port)

	f = open(filename, "w+")
	f.write("Time,FPR\n")
	f.close()

	streaming_c.foreachRDD(test)

	scc.start()
	scc.awaitTermination()