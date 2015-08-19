#!/usr/bin/python2.6
# vim: set fileencoding=utf8 :
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""High Performance Parallel 'mysqldump' loader"""

__author__ = 'tom@deepis.net (Thomas Hazel)'

import sys
import time
import string
import optparse
import MySQLdb
from threading import Thread

from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)

FLAGS = optparse.Values()
parser = optparse.OptionParser()

IGNORE_TABLES = \
[
	"`mysql`",
	"`information_schema`",
	"`performance_schema`"
]

CONNECT_OPTIONS = \
[
	"SET UNIQUE_CHECKS=0",
	"SET FOREIGN_KEY_CHECKS=0",
	"SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'"
]

PROCEDURE_OPTIONS = \
[
	"SET SQL_MODE=''"
]

def DEFINE_string(name, default, description, short_name=None):
	if default is not None and default != '':
		description = "%s (default: %s)" % (description, default)

	args = [ "--%s" % name ]
	if short_name is not None:
		args.insert(0, "-%s" % short_name)

	parser.add_option(type="string", help=description, *args)
	parser.set_default(name, default)
	setattr(FLAGS, name, default)
	return

def DEFINE_integer(name, default, description, short_name=None):
	if default is not None and default != '':
		description = "%s (default: %s)" % (description, default)

	args = [ "--%s" % name ]
	if short_name is not None:
		args.insert(0, "-%s" % short_name)

	parser.add_option(type="int", help=description, *args)
	parser.set_default(name, default)
	setattr(FLAGS, name, default)
	return

def DEFINE_boolean(name, default, description, short_name=None):
	if default is not None and default != '':
		description = "%s (default: %s)" % (description, default)

	args = [ "--%s" % name ]
	if short_name is not None:
		args.insert(0, "-%s" % short_name)

	parser.add_option(action="store_true", help=description, *args)
	parser.set_default(name, default)
	setattr(FLAGS, name, default)
	return

def ParseArgs(argv):
	usage = sys.modules["__main__"].__doc__
	parser.set_usage(usage)
	unused_flags, new_argv = parser.parse_args(args=argv, values=FLAGS)
	return new_argv

def ShowUsage():
	parser.print_help()
	return

#
# options
#

DEFINE_string('db_user', 'root', 'Database user')
DEFINE_string('db_name', 'deep', 'Target Database')
DEFINE_integer('db_port', 3306, 'Port')
DEFINE_string('db_sock', '/tmp/mysql.sock', 'Socket')
DEFINE_string('db_host', 'localhost', 'Hostname')
DEFINE_string('db_engine', 'Deep', 'Storage engine')
DEFINE_string('db_password', 'foobar', 'Database password')

DEFINE_boolean('exit', 0, 'Exit flag')
DEFINE_boolean('debug', 0, 'Print debug')

DEFINE_boolean('drop', 0, 'Drop databases')
DEFINE_integer('threads', 10, '#threads per test')

DEFINE_string('dumpfile', "dump.sql", 'file to load')

#
# loader
#

class ParseDump:

	CREATE_TABLE_STARTING = 1
	CREATE_TABLE_FINISHED = 2

	DELIMITER_STARTING = 3
	DELIMITER_FINISHED = 4

	def __init__(self):
		self.m_state = -1;
		self.m_table = None
		self.m_database = "`" + FLAGS.db_name + "`"

		self.m_createDatabase = "CREATE DATABASE IF NOT EXISTS " + self.m_database
		self.m_createTable = None
		self.m_insertValue = None

		self.m_trigger = None
		self.m_function = None
		self.m_procedure = None
		self.m_delimiterToken = None
		self.m_delimiterCommand = None
		return

	def createDatabase(self):
		createDatabase = self.m_createDatabase
		self.m_createDatabase = None
		return createDatabase

	def dropDatabase(self):
		return "DROP DATABASE IF EXISTS " + self.m_database

	def getDatabase(self):
		return self.m_database.replace("`", "", -1)

	def createTable(self):
		createTable = None
		if self.m_state == self.CREATE_TABLE_FINISHED:
			createTable = self.m_createTable
			self.m_createTable = None
		return createTable

	def dropTable(self):
		return "DROP TABLE IF EXISTS " + self.m_table

	def getTable(self):
		return self.m_table

	def insertValue(self):
		insertValue = self.m_insertValue
		self.m_insertValue = None
		return insertValue

	def delimiterCommand(self):
		delimiterCommand = None
		if self.m_state == self.DELIMITER_FINISHED:
			delimiterCommand = self.m_delimiterCommand
			self.m_delimiterCommand = None
		return delimiterCommand

	def dropProcedure(self):
		return "DROP PROCEDURE IF EXISTS " + self.m_procedure

	def getProcedure(self):
		return self.m_procedure

	def dropTrigger(self):
		return "DROP TRIGGER IF EXISTS " + self.m_trigger

	def getTrigger(self):
		return self.m_trigger

	def dropFunction(self):
		return "DROP FUNCTION IF EXISTS " + self.m_function

	def getFunction(self):
		return self.m_function;

	def parseDelimiter(self, line):
		if line.find("/*!50003 CREATE*/ ") == 0:
			functionIndex = line.find("/*!50003 FUNCTION ")
			procedureIndex = line.find("/*!50003 PROCEDURE ")
			triggerIndex = line.find("/*!50003 TRIGGER ")

			if procedureIndex >= 0:
				self.m_procedure = line[procedureIndex + len("/*!50003 PROCEDURE "):]
				line = "CREATE PROCEDURE " + self.m_procedure
				index = self.m_procedure.find("(")
				self.m_procedure = self.m_procedure[:index]
				self.m_procedure = self.m_procedure.replace('"', '`', -1)
			elif functionIndex >= 0:
				self.m_function = line[functionIndex + len("/*!50003 FUNCTION "):]
				line = "CREATE FUNCTION " + self.m_function
				index = self.m_function.find("(")
				self.m_function = self.m_function[:index]
				self.m_function = self.m_function.replace('"', '`', -1)
			elif triggerIndex >= 0:
				self.m_trigger = line[triggerIndex + len("/*!50003 TRIGGER "):]
				line = "CREATE TRIGGER " + self.m_trigger
				index = self.m_trigger.find("AFTER")
				if index < 0:
					index = self.m_trigger.find("BEFORE")

				if index > 0:
					self.m_trigger = self.m_trigger[:index]

				self.m_trigger = self.m_trigger.replace('"', '`', -1)
			else:
				print 'WARN: Unknown command ' + line

		# command might be on one line
		if line.rfind("*/" + self.m_delimiterToken) >= 0:
			line = line.replace("*/" + self.m_delimiterToken, "", -1);

		self.m_delimiterCommand = self.m_delimiterCommand + "\n" + line;
		self.m_delimiterCommand = self.m_delimiterCommand.replace('"', '`', -1)

	def loadLine(self, line):
		# set flag, don't perform comparisons during inserts (i.e. performance)
		inserting = 0

		if self.m_database:
			if line.find("INSERT INTO `") == 0 and line.endswith(";"):
				self.m_insertValue = line

				inserting = 1

			elif self.m_state == self.CREATE_TABLE_STARTING and line.find(") ENGINE=") == 0 and (line.endswith(";") or line.endswith("CHARSET=utf8")):
				if FLAGS.db_engine:
					subline = line[len(") ENGINE="):]
					index = subline.find(" ")
					engine = subline[:index]
					line = line.replace(") ENGINE=" + engine, ") ENGINE=" + FLAGS.db_engine)

				self.m_state = self.CREATE_TABLE_FINISHED
				self.m_createTable = self.m_createTable + " " + line;

			#elif self.m_state == self.CREATE_TABLE_STARTING and line.find("  `") == 0:
			#	TODO: columns
			#
			#elif self.m_state == self.CREATE_TABLE_STARTING and line.find("  PRIMARY KEY ") == 0:
			#	TODO: primary columns
			#
			#elif self.m_state == self.CREATE_TABLE_STARTING and line.find("  UNIQUE KEY `") == 0:
			#	TODO: unique secondary columns
			#
			#elif self.m_state == self.CREATE_TABLE_STARTING and line.find("  CONSTRAINT `") == 0:
			#	TODO: constraints
			#
			#elif self.m_state == self.CREATE_TABLE_STARTING and line.find("  KEY `") == 0:
			#	TODO: secondary columns
			#

			elif self.m_state == self.CREATE_TABLE_STARTING:
				self.m_createTable = self.m_createTable + " " + line;

			elif self.m_state == self.DELIMITER_STARTING:
				if line.find("DELIMITER ;") == 0:
					self.m_state = self.DELIMITER_FINISHED

				else:
					self.parseDelimiter(line)

			elif line.find("CREATE TABLE `") == 0 and line.endswith("("):
				self.m_state = self.CREATE_TABLE_STARTING

				self.m_table = line[len("CREATE TABLE ") : len(line) - 2]
				self.m_createTable = line;

			elif line.find("DELIMITER") == 0:
				self.m_state = self.DELIMITER_STARTING
				self.m_trigger = None
				self.m_function = None
				self.m_procedure = None
				self.m_delimiterCommand = ""

				index = line.find("DELIMITER")
				self.m_delimiterToken = line[index + len("DELIMITER"):].strip()

		if inserting == 0 and line.find("USE `") == 0 and line.endswith(";"):
			self.m_database = line[len("USE ") : len(line) - 1]

			if self.m_database not in IGNORE_TABLES:
				self.m_createDatabase = "CREATE DATABASE IF NOT EXISTS " + self.m_database
			else:
				self.m_database = None
		return


def createConnection(db_name=None):
	if db_name:
		connection = MySQLdb.connect(host=FLAGS.db_host, user=FLAGS.db_user, passwd=FLAGS.db_password, db=db_name, port=FLAGS.db_port, unix_socket=FLAGS.db_sock)
	else:
		connection = MySQLdb.connect(host=FLAGS.db_host, user=FLAGS.db_user, passwd=FLAGS.db_password, db=FLAGS.db_name, port=FLAGS.db_port, unix_socket=FLAGS.db_sock)

	connection.autocommit(1);

	cursor = connection.cursor()
	for command in CONNECT_OPTIONS:
		cursor.execute(command)
	cursor.close()

	return connection

def loadEnd(connections, cursor, values, newline, analyze):
	if len(values) > 0:
		values = loadRows(connections, values)
		if FLAGS.debug:
			sys.stdout.write('\n')
			newline = 0

	elif newline:
		sys.stdout.write('\n')
		newline = 0

	if analyze:
		cursor.execute("ANALYZE TABLE " + analyze)

	return values, newline, None

def loadRow(connection, tuple):
	db, values = tuple

	try:
		cursor = connection.cursor()
		cursor.execute("USE " + db)
		cursor.execute(values)
		cursor.close()
	except:
		FLAGS.exit = 1

	return

def loadRows(connections, values):
	threads = []
	for i in range(0, len(values)):
		# XXX: +1 to ignore schema connection
		t = Thread(target=loadRow, args=(connections[i + 1], values[i],))
		threads.append(t)

	for t in threads:
		t.start()

	for t in threads:
		t.join()

	return []

def loadData():
	connections = []
	# XXX: +1 to create schema connection
	for i in range(0, FLAGS.threads + 1):
		connections.append(createConnection())

	file = open(FLAGS.dumpfile, 'r')
	parser = ParseDump()

	values = []
	newline = 0
	analyze = None
	cursor = connections[0].cursor()
	while not FLAGS.exit:
		line = file.readline()
		if not line:
			break

		line = line.rstrip('\n')
		line = line.rstrip('\r')
		parser.loadLine(line)

		# values first, don't perform comparisons during inserts (i.e. performance)
		value = parser.insertValue()
		if value:
			if len(values) >= FLAGS.threads:
				values = loadRows(connections, values)

			values.append((parser.getDatabase(), value))

			if FLAGS.debug:
				sys.stdout.write('.')
				sys.stdout.flush()

			continue

		db = parser.createDatabase()
		if db:
			values, newline, analyze = loadEnd(connections, cursor, values, newline, analyze)

			if FLAGS.drop:
				if FLAGS.debug:
					print '      DROP DATABASE `' + parser.getDatabase() + "`"

				cursor.execute(parser.dropDatabase())

			if FLAGS.debug:
				print '      CREATE DATABASE `' + parser.getDatabase() + "`"

			cursor.execute(db)
			cursor.execute("USE " + parser.getDatabase())

			continue

		table = parser.createTable()
		if table:
			values, newline, analyze = loadEnd(connections, cursor, values, newline, analyze)

			# save away name for post table optimization
			analyze = parser.getTable()

			cursor.execute(parser.dropTable())
			cursor.execute(table)

			if FLAGS.debug:
				if FLAGS.db_engine:
					print '      CREATE TABLE ' + parser.getTable() + " with " + FLAGS.db_engine,
				else:
					print '      CREATE TABLE ' + parser.getTable(),

				newline = 1

			continue

		delimiterCommand = parser.delimiterCommand()
		if delimiterCommand:
			values, newline, analyze = loadEnd(connections, cursor, values, newline, analyze)

			for command in PROCEDURE_OPTIONS:
				cursor.execute(command)

			if parser.getProcedure() != None:
				if FLAGS.debug:
					print '      CREATE PROCEDURE ' + parser.getProcedure()

				cursor.execute(parser.dropProcedure())

			elif parser.getFunction() != None:
				if FLAGS.debug:
					print '      CREATE FUNCTION ' + parser.getFunction()

				cursor.execute(parser.dropFunction())

			elif parser.getTrigger() != None:
				if FLAGS.debug:
					print '      CREATE TRIGGER ' + parser.getTrigger()

				cursor.execute(parser.dropTrigger())

			cursor.execute(delimiterCommand)

			for command in CONNECT_OPTIONS:
				cursor.execute(command)

			continue

	if not FLAGS.exit:
		values, newline, analyze = loadEnd(connections, cursor, values, newline, analyze)

	cursor.close()

	for connection in connections:
		connection.close()

	file.close()

	# final newline
	if FLAGS.debug:
		if newline:
			sys.stdout.write('\n')
			sys.stdout.flush()
	return

def excuteLoader():
	loadData()
	return

def main(argv):
	if FLAGS.debug:
		print

	print 'OPEN (', "file:", FLAGS.dumpfile, ")"
	if FLAGS.debug:
		print

	start = time.time()
	excuteLoader()
	stop = time.time()

	if FLAGS.debug:
		print

	print 'CLOSE (', "time:", stop-start, ")"
	if FLAGS.debug:
		print

	return 0

if __name__ == '__main__':
	new_argv = ParseArgs(sys.argv[1:])
	sys.exit(main([sys.argv[0]] + new_argv))
