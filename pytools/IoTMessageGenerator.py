#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Deep Information Sciences, Inc. - www.deep.is
#
# Released under the Apache 2.0 License
# 
# Author: Jason Jeffords, www.linkedin.com/in/JasonJeffords, @JasonJeffords
#

import time, uuid, MySQLdb, RandomGenerator 

SOURCES = [MySQLdb.escape_string(uuid.uuid5(uuid.NAMESPACE_OID,
	".".join([`_`]*3)).bytes) for _ in xrange(32000)]
 
def _generateRecord(args=None):
	timeStamp = long(time.time()*1000)
	source = RandomGenerator.choice(SOURCES)
	message = '{"timeStamp": %s, "message": "this is a test"}' % timeStamp 
	return (timeStamp, source, message)

RECORD_FORMAT = "(%s,'%s','%s')"

ADD_RECORD_TEMPLATE = ("INSERT INTO IoTMessages "
			"(timestamp, source, message) "
			"VALUES " + RECORD_FORMAT)

generationCount = 1
def generateRecord(args=None):
	global generationCount
	generationCount += 1
	return ADD_RECORD_TEMPLATE % _generateRecord(args)

ADD_RECORDS_TEMPLATE = ("INSERT INTO IoTMessages "
			"(timestamp, source, message) "
			"VALUES %s")

def generateRecords(args=None):
	global generationCount
	generationCount += args.statementSize
	return ADD_RECORDS_TEMPLATE % \
		','.join(RECORD_FORMAT % _generateRecord(args)
		for _ in xrange(args.statementSize))

def getLastRecords(args=None):
	return "select * from IoTMessages order by timestamp desc limit " + \
		`args.statementSize`

def getLastRecordsBySource(args=None):
	return "select * from IoTMessages where source = '" + \
		RandomGenerator.choice(SOURCES) + \
		"' order by timestamp desc limit "+`args.statementSize`

def getRandomRecordsByRecordID(args=None):
	start = RandomGenerator.randint32()%(generationCount * args.concurrency)
	end = start + args.statementSize
	return "select * from IoTMessages where record_id > " + `start` + \
		" and record_id < " + `end` + \
		" order by timestamp desc limit " + `args.statementSize`
