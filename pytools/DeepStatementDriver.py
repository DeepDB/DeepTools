#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Deep Information Sciences, Inc. - www.deep.is
#
# Released under the Apache 2.0 License
# 
# Author: Jason Jeffords, www.linkedin.com/in/JasonJeffords, @JasonJeffords
#

import argparse, MySQLdb, multiprocessing, threading, time, Queue, sys, json,\
	random

def getInterval(startTime, duration):
	return startTime, startTime + duration

class DebugMessageHandler:
	def done(self, message):
		print 'done', message

	def error(self, message):
		print 'error', message

	def output(self, message):
		print 'output', message

	def status(self, message):
		print 'status', message

class CsvMessageHandler(object):
	def __init__(self, args):
		self.args = args
		self.header = None
		self.csvFile = open(args.csvFilename, "w")

	def _writeHeader(self, header, message):
		for stats in message:
			header += ", {0} min, {0} max, {0} mean, {0} count"\
				.format(stats[0])
		self.header = header+"\n"
		self.csvFile.write(self.header)

	def _writeRow(self, columns):
		columns = [`_` for _ in columns]
		self.csvFile.write(', '.join(columns)+"\n")
		self.csvFile.flush()

	def status(self, message):
		if not self.header:
			self._writeHeader("worker ID, timestamp, totalTime",
				message[1][2:])
		columns = [message[0]]
		message = message[1]
		columns += message[0:2]
		for stats in message[2:]:
			columns += stats[1:] 
		self._writeRow(columns)


class AggregateCsvMessageHandler(CsvMessageHandler):
	def __init__(self, args):
		super(AggregateCsvMessageHandler, self).__init__(args)
		self.aggregateStats = OperationalStatistics(args.functionSet)
		self.startTime, self.endTime = \
			getInterval(time.time(), self.args.reportingInterval)
		self.write = False
	
	def _write(self, timestamp):
		allStats = self.aggregateStats.getStats()
		self.aggregateStats.clear()
		self.write = False
		if not self.header:
			self._writeHeader("timestamp, totalTime", allStats)
		columns = [timestamp, timestamp-self.startTime]
		for stats in allStats:
			columns += stats[1:] 
		self._writeRow(columns)
		self.startTime, self.endTime = \
			getInterval(timestamp, self.args.reportingInterval)

	def status(self, message):
		self.aggregateStats.aggregate(message[1][2:])
		self.write = True

	def done(self, message):
		if message == 0:
			self.report(message)

	def report(self, message):
		if self.write:
			self._write(time.time())
			
class Statistics:
	def __init__(self, name, initMin=sys.float_info.max, 
		initMax=-sys.float_info.max):
		self.name = name
		self.initMin = initMin
		self.initMax = initMax
		self.clear()

	def clear(self):
		self.min = self.initMin
		self.max = self.initMax
		self.mean = 0.0
		self.count = 0

	def addSample(self, sample):
		total = self.count * self.mean
		self.count += 1
		self.mean = (total + sample)/self.count
		if sample > self.max:
			self.max = sample
		if sample < self.min:
			self.min = sample

	def aggregate(self, min, max, mean, count):
		total = (self.count * self.mean) + (mean * count)
		self.count += count
		if self.count > 0:
			self.mean = total/self.count
		if max > self.max:
			self.max = max
		if min < self.min:
			self.min = min

	def getStats(self):
		return self.name, self.min, self.max, self.mean, self.count

class OperationalStatistics:
	def __init__(self, functionSet):
		self.allStats = []
		self.transactionLatency =self._createStats('transactionLatency')
		self.commitLatency = self._createStats('commitLatency')
		self.statementLatency = self._createStats('statementLatency')
		self.retryLatency = self._createStats('retryLatency')
		self.statsMap = {}
		for f in args.functionSet: # ensure all functional stats exist
			self.getStatsByFunction(f)

	def _createStats(self, name):
		stats = Statistics(name)
		self.allStats.append(stats)
		return stats

	def getStatsByFunction(self, function):
		try:
			return self.statsMap[function]
		except:
			stats = self._createStats(
				function.__module__+'.'+function.__name__)
			self.statsMap[function] = stats
			return stats

	def clear(self):
		for s in self.allStats:
			s.clear()
	
	def getStats(self):
		return tuple([s.getStats() for s in self.allStats])

	def aggregate(self, stats):
		for i, s in enumerate(self.allStats):
			s.aggregate(*(stats[i][1:]))

class Configuration:
	def __init__(self, **args):
		self.__dict__.update(args)

class Channel:
	def __init__(self, queue=multiprocessing.Queue(), messageHandlers=[]):
		self.queue = queue
		self.messageHandlers = messageHandlers
		self.senders = 0

	def addSender(self):
		self.senders += 1

	def sendMessage(self, *message):
		self.queue.put(message)

	def sendDone(self, fromID):
		self.sendMessage('done', fromID)
	
	def sendError(self, fromID, textMessage):
		self.sendMessage('error', fromID, textMessage)

	def sendOutput(self, fromID, textMessage):
		self.sendMessage('output', fromID, textMessage)

	def sendStatus(self, fromID, *statusMessage):
		self.sendMessage('status', fromID, statusMessage)

	def sendReport(self, fromID):
		self.sendMessage('report', fromID)

	def processMessages(self):
		while self.senders > 0:
			message = self.queue.get()
			messageType = message[0]
			for handler in self.messageHandlers:
				try:
					args = message[1:]
					getattr(handler, messageType)(args)
				except (AttributeError), e:
					pass
			if messageType == 'done':
				self.senders -= 1
		for handler in self.messageHandlers:
			try:
				handler.done(0)
			except (AttributeError), e:
				pass
		return

class Worker:
	def __init__(self, id, channel, args):
		self.id = id
		self.channel = channel
		self.args = args
		self.ops = OperationalStatistics(args.functionSet)
		channel.addSender()
		self.workQueue = Queue.Queue()
		self.process = startProcess(self.executeStatements, ())

	def produceStatements(self):
		if not self.args.statementCount:
			self._produceContinuousStatements()
		else:
			self._produceUpToStatementCount()
		self.workQueue.put(None) # sentinel indicating done

	def _getFunctionAndStatement(self):
		function = random.choice(self.args.functionTable)
		return function, function(self.args)

	def _generateStatements(self, upToSize):
		statements = []
		for _ in xrange(upToSize):
			function, statement = self._getFunctionAndStatement()
			if statement == None:
				break
			statements.append((function, statement))
		return statements 

	def _produceUpToStatementCount(self):
		statementCount = self.args.statementsPerThread + \
			(self.id <= self.args.remainingStatements)
		self.channel.sendOutput(self.id, "Worker " + `self.id` + 
			" executing " + `statementCount` + " statements.")
		loopCount, remainder = divmod(statementCount, 
			self.args.commitBatchSize)
		for _ in xrange(loopCount):
			self.workQueue.put(
				self._generateStatements(
					self.args.commitBatchSize))
		if remainder:
			self.workQueue.put(self._generateStatements(remainder))

	def _produceContinuousStatements(self):
		self.channel.sendOutput(self.id, "Worker " + `self.id` + 
			" executing continuous statements.")
		while True:
			statements = self._generateStatements(
				self.args.commitBatchSize)
			self.workQueue.put(statements)
			if len(statements) < self.args.commitBatchSize:
				break

	def _executeTransaction(self, statements, cursor):
		txnStartTime = time.time()
		for function, statement in statements:
			while True: # automatically retry on exceptions
				statementStartTime = time.time()
				try:
					cursor.execute(statement)
					latency = time.time()-statementStartTime
					self.ops.statementLatency.addSample(
						latency)
					self.ops.getStatsByFunction(function)\
						.addSample(latency)
					break # break on success
				except (Exception), e:
					self.ops.retryLatency.addSample(\
						time.time()-statementStartTime)
					self.channel.sendError(self.id, `e`)
		commitStartTime = time.time()
		self.connection.commit()
		txnEndTime = time.time()
		self.ops.commitLatency.addSample(txnEndTime - commitStartTime)
		self.ops.transactionLatency.addSample(txnEndTime - txnStartTime)
		self.workQueue.task_done()
		return txnEndTime

	def _report(self, txnEndTime, startTime):
		self.channel.sendStatus(self.id, *((txnEndTime, 
			txnEndTime-startTime) + self.ops.getStats()))
		self.ops.clear()
		return getInterval(time.time(), self.args.reportingInterval)

	def _rateLimit(self, initialStartTime, loopCount):
		totalLoopTime = time.time() - initialStartTime
		totalElements = self.args.statementSize * \
			self.args.commitBatchSize*loopCount
		elementsSecond = totalElements/totalLoopTime
		if elementsSecond > self.args.targetRate:
			sleepTime = random.random() *\
				(totalElements/self.args.targetRate - \
				totalLoopTime)
			time.sleep(sleepTime)

	def _consumeWork(self, cursor):
		startTime, reportingEndTime = \
			getInterval(time.time(), self.args.reportingInterval)
		txnEndTime = initialStartTime = startTime
		reported = False
		loopCount = 0
		statements = self.workQueue.get()
		while statements:
			loopCount+=1
			reported = False
			txnEndTime = self._executeTransaction(
				statements, cursor)
			if txnEndTime >= reportingEndTime:
				startTime, reportingEndTime = \
					self._report(txnEndTime, startTime)
				reported = True
			if self.args.targetRate: # rate limit
				self._rateLimit(initialStartTime, loopCount)
			statements = self.workQueue.get()
				
		if not reported: # send final report
			self._report(txnEndTime, startTime)
			
		# signal this worker is done to parent
		self.channel.sendDone(self.id)

		# indicate done reading sentinel
		self.workQueue.task_done() 

	def consumeWork(self):
		with self.connection as cursor:
			self._consumeWork(cursor)
		
	def executeStatements(self):
		self.args.workerID = self.id
		self.connection = MySQLdb.connect(host=self.args.host, 
			port=self.args.port, user=self.args.username, 
			passwd=self.args.password, db=self.args.databaseSchema)
		# queue maxsize is chosen to ensure queue is never empty while
		# rate limiting statement generation
		self.workQueue = Queue.Queue(maxsize=4) 
		producer = startThread(self.produceStatements, ())
		startThread(self.consumeWork, (), True)
		producer.join()
		self.workQueue.join()

def executeSQLScript(args, fileName):
	with open(fileName, "r") as script:
		executeSQL(args, script.read())

def executeSQL(args, sql):
	with MySQLdb.connect(host=args.host, port=args.port, 
		user=args.username, passwd=args.password, db=args.databaseSchema) as cursor:
		statements = sql.split(";")
		for statement in statements: 
			if statement.strip(): #real statement, not blank line(s)
				cursor.execute(statement)

def _start(constructor, target, args, daemon):
	a = constructor(target=target, args=args)
	a.daemon = daemon
	a.start()
	return a

def startThread(target, args, daemon=False):
	return _start(threading.Thread, target, args, daemon)

def startProcess(target, args, daemon=True):
	return _start(multiprocessing.Process, target, args, daemon)

def getFunction(moduleAndFunctionName):
	tokens = moduleAndFunctionName.split('.')
	moduleName = '.'.join(tokens[0:-1])
	functionName = tokens[-1] 
	return getattr(__import__(moduleName), functionName)


parser = argparse.ArgumentParser(
	description = "Generates statements using the specified statement"
		" generator and applies the generated statements to the"
		" specified schema.", 
	epilog = "The resulting statements are executed against the specified"
		" database schema without intermediate storage.")

parser.add_argument('-d', '--databaseSchema', help='database schema') 
parser.add_argument('-t', '--tableName', 
	help='the name of the table to use, passed in args to generation '
		'function') 
parser.add_argument('-sc', '--statementCount', default=0, type=int, 
	help='the total number of statements to execute, 0 for continuous'
		' execution') 
parser.add_argument('-g', '--generationFunction', 
	help='the python statement generation function in module.function'
		' format, takes args')
parser.add_argument('-py', '--pythonPath', default=None, 
	help='python path for the generation function module and any other'
		' required modules') 
parser.add_argument('-host', '--host', default='localhost', 
	help='the database host name or IP') 
parser.add_argument('-port', '--port', default=3306, type=int, 
	help='the database server port') 
parser.add_argument('-u', '--username', default='root', 
	help='database user name') 
parser.add_argument('-p', '--password', default='foobar', 
	help='database password') 
parser.add_argument('-ss', '--statementSize', default=1, type=int, 
	help='number of elements within a statement, passed in args to'
		' generation function') 
parser.add_argument('-cs', '--commitBatchSize', default=1, type=int, 
	help='the number of statements within each transaction commit') 
parser.add_argument('-c', '--concurrency', default=multiprocessing.cpu_count(),
	type=int, help='the transaction level concurrency') 
parser.add_argument('-b', '--beforeSQL', 
	help='a SQL script to run before executing statements') 
parser.add_argument('-a', '--afterSQL', 
	help='a SQL script to run after executing statements') 
parser.add_argument('-csv', '--csvFilename', 
	help='write results to the specified file in csv format') 
parser.add_argument('-ri', '--reportingInterval', default=1.0, type=float, 
	help='status reporting interval in fractional seconds') 
parser.add_argument('-G', '--generationConfigFile', 
	help='the statement generation configuration file in JSON format') 
parser.add_argument('-v', '--visualize', default=False, 
	help='a simple curses visualization (work in progress)') 
parser.add_argument('-D', '--debug', default=False, 
	help='output debugging messages') 
parser.add_argument('-rl', '--rateLimit', default=0.0, type=float, 
	help='limit the rate of elements per second to this fractional value'
		' in seconds') 

if len(sys.argv) <= 1: # no arguments given
        parser.print_usage()
        exit()

args = parser.parse_args()

if args.beforeSQL:
	executeSQLScript(args, args.beforeSQL)

print "Executing " + (`args.statementCount` if args.statementCount else \
	"continuous") + " statements within " + args.databaseSchema + \
	" with concurrency of " + `args.concurrency` + "."

if args.pythonPath:
	sys.path.append(args.pythonPath)
args.statementsPerThread, args.remainingStatements = \
	divmod(args.statementCount, args.concurrency)

args.generationConfiguration = []
if args.generationFunction:
	args.generationConfiguration.append(
		Configuration(functionName=args.generationFunction,frequency=1))

if args.generationConfigFile: 
	# load JSON config file into python object list
	args.generationConfiguration += json.loads(\
		open(args.generationConfigFile, 'r').read(),\
		object_hook=lambda d: Configuration(**d))

args.functionSet = [] # configuration ordered set of functions
args.functionTable = [] # function frequency table
for config in args.generationConfiguration: 
	f = getFunction(config.functionName)
	if f not in args.functionSet:
		args.functionSet.append(f)
	args.functionTable += [f]*config.frequency

messageHandlers = []

if args.debug:
	messageHandlers.append(DebugMessageHandler())

if args.visualize: # TODO not implemented
	import CursesMessageHandler
	messageHandlers.append(CursesMessageHandler.ViewMessageHandler())

if args.csvFilename:
	messageHandlers.append(AggregateCsvMessageHandler(args))

channel = Channel(messageHandlers=messageHandlers)

def report(channel, reportingInterval):
	while True:
		time.sleep(reportingInterval)
		channel.sendReport(0)

if args.reportingInterval:
	startThread(report, (channel, args.reportingInterval), True)

args.targetRate = None
if args.rateLimit > 0.0: # calculate the target rate for each worker
	args.targetRate = args.rateLimit/args.concurrency

startTime = time.time()
for i in xrange(1, args.concurrency+1):
	Worker(i, channel, args)

channel.processMessages() # blocks waiting until all work is done

totalTime = time.time() - startTime

print "Executed " + `args.statementCount` + " statements in " + `totalTime` +\
	" seconds, " + `args.statementCount / totalTime` + " statements/sec, "+\
	`(args.statementCount * args.statementSize) / totalTime ` +\
	" elements/sec."

if args.afterSQL:
	executeSQLScript(args, args.afterSQL)
