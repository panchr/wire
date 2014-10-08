# Rushy Panchal
# wire/database.py
# The Database class is a wrapper for the sqlite3.Connection class

import sqlite3
import atexit

from sqlstring import SQLString
from cursor import ExecutionCursor

class Database(sqlite3.Connection):
	'''Database interface for SQLite'''
	def __init__(self, path, *args, **kwargs):
		'''Opens an SQLite database (or creates it if it does not exist)

		Arguments:
			path - path to database

		Usage:
			db = Database("test.db")

		returns a Database (wrapper to sqlite3.Connection) object'''
		sqlite3.Connection.__init__(self, path, *args, **kwargs)
		self.path = path
		self.cursors = {}
		self.reset_counter = 0
		self.defaultTable = None
		self.debug = False
		atexit.register(self.close)

	def toggle(self, option):
		'''Toggles an option

		Arguments:
			option - name of option to toggle

		Usage:
			db.toggle("debug")

		Returns the new value of the option'''
		new_value = not getattr(self, option)
		setattr(self, option, new_value)
		return new_value

	@staticmethod
	def create(name, file_path):
		'''Creates an SQLite database from an SQL file

		Arguments:
			name - name of new database
			file_path - path to the file

		Usage:
			db = Database.create("db_commands.sql")

		returns a Database (wrapper to sqlite3.Connection) instance'''
		db = Database(name)
		db.executeFile(file_path)
		return db

	def newCursor(self):
		'''Creates a new SQLite cursor --- deprecated, use Database.cursor() instead

		Arguments:
			None

		Usage:
			cursor = db.newCursor()

		returns an sqlite3.Cursor'''
		return self.cursor()

	def purgeCursors(self):
		'''Deletes all of the stored cursors --- deprecated but maintained for backwards compatability

		Arguments:
			None

		Usage:
			db.purgeCursors()

		returns None'''
		return None

	def transaction(self):
		'''Starts a new database transaction

		Arguments:
			None

		Usage:
			trans = db.transaction()

		returns a Transaction object'''
		return Transaction(self)

	def execute(self, cmd, *args, **kwargs):
		'''Executes an SQL command

		Arguments:
			cmd - SQL command string

		Usage:
			query = db.execute("SELECT * FROM MY_TABLE")

		returns an ExecutionCursor object'''
		if self.debug:
			print(cmd, args, kwargs)
		exec_cursor = self.cursor()
		exec_cursor.execute(cmd, *args, **kwargs)
		self.commit()
		return ExecutionCursor(exec_cursor)

	def query(self, cmd, *args, **kwargs):
		'''Executes an SQL command
		
		This is the same as self.execute'''
		return self.execute(cmd, *args, **kwargs)

	def pragma(self, cmd):
		'''Executes an SQL PRAGMA function

		Arguments:
			cmd - command string

		Usage:
			query = db.pragma("INTEGRITY_CHECK")

		returns an ExecutionCursor object'''
		query = SQLString.pragma(cmd)
		return self.execute(query)

	def executeFile(self, file_path):
		'''Executes the commands from a file path; each command is delimited with a semicolon

		Arguments:
			file_path - path of file to execute

		Usage:
			query = db.executeFile("my_commands.sql")

		returns an ExecutionCursor object'''
		with open(file_path, 'r') as sql_file:
			sql_script = sql_file.read()
		return self.script(sql_script)

	def script(self, sql_script):
		'''Executes an SQLite script

		Arguments:
			sql_script - a string of commands, each separated by a semicolon

		Usage:
			query = db.script("""CREATE TABLE users (id INT NOT NULL, USERNAME VARCHAR(50));
				INSERT INTO users (1, 'panchr');""")

		returns an ExecutionCursor object'''
		exec_cursor = self.cursor()
		exec_cursor.executescript(sql_script)
		self.commit()
		return ExecutionCursor(exec_cursor)

	def count(self):
		'''Finds the number of rows created, modified, or deleted during this database connection

		Arguments:
			None

		Usage:
			affected = db.count()

		returns current affected rows'''
		return self.total_changes() - self.reset_counter

	def resetCounter(self):
		'''Resets the rows counter

		Arguments:
			None

		Usage:
			db.resetCounter()

		returns None'''
		self.reset_counter = self.count()

	def checkIntegrity(self, max_errors = 100):
		'''Checks the Database Integrity 

		Arguments:
			max_errors (default: 100) - number of maximum errors to be displayed

		Usage:
			errors = db.checkIntegrity()

		returns True if there are no errors, or a list of errors'''
		query = SQLString.checkIntegrity(max_errors)
		results = self.execute(query).fetch()
		return True if (len(results) == 1 and results[0]["integrity_check"] == "ok") else results

	def fetch(self, cursor, type_fetch = "all", type = dict):
		'''Fetches columns from the cursor

		Arguments:
			cursor - cursor instance
			type_fetch - how many to fetch (all, one)
			type - type of fetch (dict, list)'''
		return ExecutionCursor(cursor).fetch(type_fetch, type)

	def tables(self, objects = False, temp = False):
		'''Shows the tables in the database

		Arguments:
			objects - whether or not to return as Table objects (defaults to False)
			temp - whether or not to include Temporary tables

		Usage:
			tables = db.tables()

		returns a list of table names'''
		if temp:
			query = "SELECT name FROM (SELECT * FROM sqlite_master UNION SELECT * FROM sqlite_temp_master) WHERE type = 'table' ORDER BY name"
		else:
			query = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
		names = map(lambda item: item["name"], self.execute(query).fetch())
		return map(lambda table: Table(self, table, False), names) if objects else names

	def table(self, name, verify = True):
		'''Creates a Table object

		Arguments:
			name - name of the table

		Usage:
			table = db.table("users")

		returns a Table object'''
		return Table(self, name, verify)

	def tableExists(self, name, temp = False):
		'''Checks if a database table exists 

		Arguments:
			name - name of table to check
			temp - whether or not to check temporary tables

		Usage:
			table_exists = db.tableExists("users")

		returns True if the table exists or False'''
		return name in self.tables(temp = temp)

	def setTable(self, table):
		'''Sets the default table to use for queries

		Arguments:
			table - name of default table

		Usage:
			db.setTable("users")

		returns None'''
		self.defaultTable = table

	def createTable(self, name, temporary = False, **columns):
		'''Creates a table in the database

		see Table.create for further reference'''
		return Table.create(self, name, temporary, **columns)

	def dropTable(self, name):
		'''Drops a table from the database

		see Table.drop for further reference'''
		query = SQLString.dropTable(name)
		return self.execute(query)

	def insert(self, table = None, **columns):
		'''Insert rows into the table

		Arguments:
			table - table name to insert into
			**columns - dictionary of column names and values {column_name: value, ...}

		Usage:
			db.insert("users", id = 1, username = "panchr"))

		returns an ExecutionCursor object'''
		if not table:
			table = self.defaultTable
		query, values = SQLString.insert(table, **columns)
		return self.execute(query, values)

	def update(self, table = None, equal = None, like = None, where = "1 = 1", **columns):
		'''Updates rows in the table

		Arguments:
			table - name of table to update
			equal - dictionary of columns and values to use in WHERE  + "=" clauses {column_name: value, ...}
			like - dictionary of columns and values to use in WHERE + LIKE clauses (column_name: pattern, ...}
			where - custom WHERE and/or LIKE clause(s)
			**columns - dictionary of column names and values {column_name: value, ...}

		Usage:
			db.update("table", equal = {"id": 5}, username = "new_username")

		returns an ExecutionCursor object'''
		if not table:
			table = self.defaultTable
		query, values = SQLString.update(table, equal, like, where, **columns)
		return self.execute(query, tuple(values))

	def select(self, table = None, **options):
		'''Selects rows from the table

		Arguments:
			table - table name to select from
			columns - a list of columns (use ALL for all columns)
			equal - dictionary of columns and values to use in WHERE  + "=" clauses {column_name: value, ...}
			like - dictionary of columns and values to use in WHERE + LIKE clauses (column_name: pattern, ...}
			where - custom WHERE and/or LIKE clause(s)

		Usage:
			query = db.select("users", columns = ALL, equal = {"id": 1}, like = {"username": "pan%"})
			query = db.select("users", columns = ALL, where = "`ID` = 1 OR `USERNAME` LIKE 'pan%'")

		returns an ExecutionCursor object'''
		if not table:
			table = self.defaultTable
		query, values = SQLString.select(table, **options)
		return self.execute(query, values)

	def delete(self, table = None, **options):
		'''Deletes rows from the table

		Arguments:
			table - name of table to delete from
			equal - dictionary of columns and values to use in WHERE  + "=" clauses {column_name: value, ...}
			like - dictionary of columns and values to use in WHERE + LIKE clauses (column_name: pattern, ...}
			where - custom WHERE and/or LIKE clause(s)

		Usage:
			db.delete("users", equal = {"id": 5})

		returns an ExecutionCursor object'''
		if not table:
			table = self.defaultTable
		query, values = SQLString.delete(table, **options)
		return self.execute(query, values)

class Transaction(Database):
	'''Models an SQL transaction'''
	def __init__(self, db):
		'''Creates the Transaction object

		Arguments:
			db - Database instance to use

		Usage:
			trans = Transaction(db)

		returns the Transaction object'''
		self.db = db
		self.cursor = db.newCursor()
		self.fetchall, self.fetchone, self.description = self.cursor.fetchall, self.cursor.fetchone, self.cursor.description
		self.rollback = self.db.rollback
		self.reset_counter = 0
		self.defaultTable = None
		self.debug = False

		def raiseError():
			'''Helper function --- raises an attribute error'''
			raise AttributeError("Attribute does not exist")

		for method in ["transaction", "newCursor", "purgeCursors", "create"]:
			if hasattr(self, method):
				setattr(self, method, lambda *args, **kwargs: raiseError())

	def execute(self, cmd, *args, **kwargs):
		'''Executes an SQL command

		See Database.execute for further reference'''
		if self.debug:
			print(cmd, args, kwargs)
		self.cursor.execute(cmd, *args, **kwargs)

	def commit(self):
		'''Commits the changes to the database

		see sqlite3.Connection.commit for further reference

		Returns an ExecutionCursor object'''
		self.db.commit()
		return ExecutionCursor(self.cursor)

	def count(self):
		'''Returns the affected rows since last reset

		See Database.count for further reference'''
		return self.cursor.rowcount - self.reset_counter
