# pysqlite.py
# Rushy Panchal
# Copyright 2014
# Provides an interface for SQLite databases, using the sqlite3 module

### Imports

import sqlite3
import time
import csv

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
		self.path = path
		self.cursors = {}
		self.reset_counter = 0
		self.table = None
		self.debug = False

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
		'''Creates a new SQLite cursor

		Arguments:
			None

		Usage:
			cursor = db.newCursor()

		returns a sqlite3.Cursor instance'''
		new_cursor = self.cursor()
		cursor_id = hex(int(1000 * time.time())) + hex(id(new_cursor))
		self.cursors[cursor_id] = new_cursor
		return new_cursor

	def purgeCursors(self):
		'''Deletes all of the stored cursors

		Arguments:
			None

		Usage:
			db.purgeCursors()

		returns None'''
		for cursor_id, cursor in self.cursors.iteritems():
			del self.cursors[cursor_id]
			del cursor

	def execute(self, cmd, *args, **kwargs):
		'''Executes an SQL command

		Arguments:
			cmd - SQL command string

		Usage:
			query = db.execute("SELECT * FROM MY_TABLE")

		returns a sqlite3.Cursor instance'''
		if self.debug:
			print cmd, args, kwargs
		exec_cursor = self.newCursor()
		exec_cursor.execute(cmd, *args, **kwargs)
		self.commit()
		return ExecutionCursor(exec_cursor)

	def query(self, cmd, *args, **kwargs):
		'''Executes an SQL command
		
		This is the same as self.execute'''
		return self.execute(cmd, *args, **kwargs)

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
		exec_cursor = self.newCursor()
		exec_cursor.executescript(sql_script)
		self.commit()
		return ExecutionCursor(exec_cursor)

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

	def dropTable(self, name):
		'''Drops a table from the database

		Arguments:
			name - name of the table to be dropped

		Usage:
			db.dropTable("users")

		returns an sqlite3.Cursor instance'''
		query = "DROP TABLE {table}".format(table = name)
		return self.execute(query)

	def setTable(self, table):
		'''Sets the default table to use for queries

		Arguments:
			table - name of default table

		Usage:
			db.setTable("users")

		returns None'''
		self.table = table

	def fetch(self, cursor, type_fetch = "all", type = dict):
		'''Fetches columns from the cursor

		Arguments:
			cursor - cursor instance
			type_fetch - how many to fetch (all, one)
			type - type of fetch (dict, list)'''
		return ExecutionCursor(cursor).fetch(type_fetch, type)

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
		values = tuple(columns[column.replace('`', '')] for column in column_names)
		value_string = '?, ' * (len(values) - 1) + '?'
		query = "INSERT INTO {table} ({columns}) VALUES ({values})".format(table = table, columns = ','.join(column_names), values = value_string)
		return self.execute(query, values)

	def update(self, table = None, equal = None, like = None, where = "1 = 1", **columns):
		'''Updates columns in the table

		Arguments:
			table - name of table to update
			equal - dictionary of columns and values to use in WHERE  + "=" clauses {column_name: value, ...}
			like - dictionary of columns and values to use in WHERE + LIKE clauses (column_name: pattern, ...}
			where - custom WHERE and/or LIKE clause(s)
			columns - dictionary of column names and values {column_name: value, ...}

		Usage:
			db.update("table", equal = {"id": 5}, username = "new_username")

		returns a sqlite3.Cursor instance'''
		if not table:
			table = self.table
		if not equal:
			equal = {}
		if not like:
			like = {}
		column_items = columns.items()
		column_str = ','.join("`{column}` = ?".format(column = column[0]) for column in column_items)
		values = [column[1] for column in column_items]
		like_items = like.items()
		like_str = ' AND '.join("`{column}` LIKE ?".format(column = column[0]) for column in like_items)
		values.extend([column[1] for column in like_items])
		equal_items = equal.items()
		equal_str = ' AND '.join("`{column}` = ?".format(column = column[0]) for column in equal_items)
		values.extend([column[1] for column in equal_items])
		where = ' AND '.join(filter(lambda item: bool(item), [like_str, equal_str, where]))
		query = "UPDATE {table} SET {columns} WHERE {where}".format(table = table, columns = column_str, where = where)
		return self.execute(query, tuple(values))

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
			query = db.select("users", columns = ALL, where = "`ID` = 1 OR `USERNAME` LIKE 'pan%'")

		returns a sqlite3.Cursor instance'''
		if not table:
			table = self.table
		columns = ','.join('`{column}`'.format(column = column) for column in options.get('columns', ALL))
		if columns == "`*`":
			columns = "*"
		like_items = options.get('like', {}).items()
		like_str = ' AND '.join("`{column}` LIKE ?".format(column = column[0]) for column in like_items)
		values = [column[1] for column in like_items]
		equal_items = options.get('equal', {}).items()
		equal_str = ' AND '.join("`{column}` = ?".format(column = column[0]) for column in equal_items)
		values.extend([column[1] for column in equal_items])
		where = ' AND '.join(filter(lambda item: bool(item), [like_str, equal_str, options.get('where', '1 = 1')]))
		query = "SELECT {columns} FROM {table} WHERE {where}".format(columns = columns, table = table, where = where)
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

		returns a sqlite3.Cursor instance'''
		if not table:
			table = self.table
		like_items = options.get('like', {}).items()
		like_str = ' AND '.join("`{column}` LIKE ?".format(column = column[0]) for column in like_items)
		values = [column[1] for column in like_items]
		equal_items = options.get('equal', {}).items()
		equal_str = ' AND '.join("`{column}` = ?".format(column = column[0]) for column in equal_items)
		values.extend([column[1] for column in equal_items])
		where = ' AND '.join(filter(lambda item: bool(item), [like_str, equal_str, options.get('where', '1 = 1')]))
		query = "DELETE FROM {table} WHERE {where}".format(table = table, where = where)
		return self.execute(query, values)

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

class ExecutionCursor(object):
	'''Provides additional functionality to the sqlite3.Cursor object'''
	def __init__(self, cursor):
		self.cursor = cursor
		self.fetchall, self.fetchone, self.description = self.cursor.fetchall, self.cursor.fetchone, self.cursor.description
		self.fetched = None

	def fetch(self, type_fetch = "all", type = dict):
		'''Fetches columns from the cursor

		Arguments:
			type_fetch - how many to fetch (all, one)
			type - type of fetch (dict, list)'''
		if self.fetched:
			return self.fetched
		rows = self.cursor.fetchall() if type_fetch == "all" else [self.cursor.fetchone()]
		if type == dict:
			return_value =  [{column[0]: row[index] for index, column in enumerate(self.cursor.description)} for row in rows]
		else:
			return_value =  rows
		self.fetched = return_value
		return return_value

	def export(self, filepath = "sqlite_export.csv"):
		'''Exports the results to a CSV (Comma separated values) file

		Arguments:
			filepath - path of the CSV file(defaults to "sqlite_export.csv")

		Usage:
			db.export("mytable.csv")

		returns None'''
		with open(filepath, 'wb') as csv_file:
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow([header[0] for header in self.description])
			csv_writer.writerows(self.cursor)

### Misc. Functions

def escapeString(value):
	'''Escapes a string'''
	return "'{string}'".format(string = value) if isinstance(value, basestring) else value