# pysqlite.py
# Rushy Panchal
# Copyright 2014
# Provides an interface for SQLite databases, using the sqlite3 module

### Imports

import sqlite3
import time

### Constants

ALL = "*"

### Main classes

class Database(sqlite3.Connection):
	'''Database interface for SQLite'''
	def __init__(self, path, *args, **kwargs):
		'''Opens an SQLite database (or creates it if it does not exist)

		Arguments:
			path - path to database

		Usage:
			db = Database("test.db")

		returns a Database (wrapper to sqlite3.Connection) instance'''
		sqlite3.Connection.__init__(self, path, *args, **kwargs)
		self.cursors = {}
		self.reset_counter = 0
		self.table = None

	@staticmethod
	def create(self, file_path):
		'''Creates an SQLite database from an SQL file

		Arguments:
			file_path - path to the file

		Usage:
			db = Database.create("db_commands.sql")

		returns a Database (wrapper to sqlite3.Connection) instance'''
		return self.executeFile(file_path)

	def new_cursor(self):
		'''Creates a new SQLite cursor

		returns a sqlite3.Cursor instance'''
		new_cursor = self.cursor()
		cursor_id = hex(int(1000 * time.time())) + hex(id(new_cursor))
		self.cursors[cursor_id] = new_cursor
		return new_cursor

	def execute(self, cmd, *args, **kwargs):
		'''Executes an SQL command

		Arguments:
			cmd - SQL command string

		Usage:
			query = db.execute("SELECT * FROM MY_TABLE")

		returns a sqlite3.Cursor instance'''
		print cmd
		exec_cursor = self.new_cursor()
		exec_cursor.execute(cmd, *args, **kwargs)
		self.commit()
		return exec_cursor

	def executeFile(self, file_path):
		'''Executes the commands from a file path; each command is delimited with a semicolon

		Arguments:
			file_path - path of file to execute

		Usage:
			query = db.executeFile("my_commands.sql")

		returns a sqlite3.Cursor instance'''
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

		returns a sqlite3.Cursor instance'''
		exec_cursor = self.new_cursor()
		exec_cursor.executescript(sql_script)
		self.commit()
		return exec_cursor

	def createTable(self, name, **columns):
		'''Creates a table in the database

		Arguments:
			name - table name
			columns - dictionary of column names and value types (or a list of [type, default])
				{column_name: value_type, other_column: [second_type, default_value], ...}

		Usage:
			db.createTable("users", "id" = "INT", username =  ["VARCHAR(50)", "user"])

		returns a sqlite3.Cursor instance'''
		for column, value in columns.iteritems():
			if isinstance(value, list):
				columns[column] = "{type} DEFAULT {default}".format(type = value[0], default = value[1])
			else:
				columns[column] = "{type}{other}".format(type = value, other = " NOT NULL" if value.lower() != "null" else "")
		query = "CREATE TABLE {table} ({columns})".format(table = name, columns = ','.join(column + ' ' + columns[column] for column in columns))
		return self.execute(query)

	def insert(self, table = None, **columns):
		'''Inserts "columns" into "table"

		Arguments:
			table - table name to insert into
			columns - dictionary of column names and values {column_name: value, ...}

		Usage:
			db.insert("users", id = 1, username = "panchr"))

		returns a sqlite3.Cursor instance'''
		if not table:
			table = self.table
		column_names = ['`{column}`'.format(column = column) for column in columns.keys()]
		values = map(escapeString, [columns[column.replace('`', '')] for column in column_names])
		print values
		query = "INSERT INTO {table} ({columns}) VALUES ({values})".format(table = table, columns = ','.join(column_names), values = ','.join(map(str, values)))
		return self.execute(query)

	def select(self, table = None, **options):
		'''Selects "columns" from "table"

		Arguments:
			table - table name to select from
			columns - a list of columns (use ALL for all columns)
			equal - dictionary of columns and values to use in WHERE  + "=" clauses {column_name: value, ...}
			like - dictionary of columns and values to use in WHERE + LIKE clauses (column_name: pattern, ...}
			where - custom WHERE and/or LIKE clause(s)

		Usage:
			query = db.select("users", columns = ALL, equal = {"id": 1}, like = {"username": "pan%"})
			query = db.select("users", columns = ALL, clause = "`ID` = 1 OR `USERNAME` LIKE 'pan%'")

		returns a sqlite3.Cursor instance'''
		if not table:
			table = self.table
		columns = ','.join(options.get('columns', ALL))
		like = ' AND '.join("`{column}` LIKE '{pattern}'".format(column = column, pattern = pattern) for column, pattern in options.get('like', {}).iteritems())
		equal = ' AND '.join('`{column}` = {pattern}'.format(column = column, pattern = escapeString(pattern)) for column, pattern in options.get('equal', {}).iteritems())
		where = ' AND '.join(filter(lambda item: bool(item), [like, equal, options.get('where', '1 = 1')]))
		query = "SELECT {columns} FROM {table} WHERE {where}".format(columns = columns, table = table, where = where)
		return self.execute(query)

	def setTable(self, table):
		'''Sets the default table to use for queries

		Arguments:
			table - name of default table

		Usage:
			db.setTable("users")

		returns None'''
		self.table = table

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

### Misc. Functions

def escapeString(value):
	'''Escapes a string'''
	return "'{string}'".format(string = value) if isinstance(value, str) else value
