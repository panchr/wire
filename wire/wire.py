# pysqlite.py
# Rushy Panchal
# Copyright 2014
# Provides an interface for SQLite databases, using the sqlite3 module

### Imports

import sqlite3
import atexit
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
		'''Creates a new SQLite cursor

		Arguments:
			None

		Usage:
			cursor = db.newCursor()

		returns an sqlite3.Cursor'''
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
		for cursor_id, cursor in self.cursors.tems():
			del self.cursors[cursor_id]
			del cursor

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
		exec_cursor = self.newCursor()
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
		exec_cursor = self.newCursor()
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

class Table(object):
	'''Models a Database table'''
	def __init__(self, db, table, verify = True):
		'''Creates the Table object

		Arguments:
			db - Database object
			table - table name to use
			verify - whether or not to check if the table exists

		Usage:
			table = Table(db, "users")

		returns the Table object'''
		self.db, self.name = db, table
		self.execute = self.db.execute
		# print self.db.tableExists(table, temp = True), verify
		if verify and not self.db.tableExists(table, temp = True):
			raise ValueError('Table {name} does not exist in database'.format(name = table))

	def info(self):
		'''Retrieves the table info in the table

		Arguments:
			None

		Usage:
			info = db.table("users").info()

		returns an ExecutionCursor object'''
		return self.db.pragma("table_info({table})".format(table = self.name))

	def columns(self):
		'''Retrieves the columns in the table

		Arguments:
			None

		Usage:
			columns = db.table("users").columns()

		returns a list of columns'''
		return map(lambda item: item["name"], self.info().fetch())

	@staticmethod
	def create(db, name, temporary = False,**columns):
		'''Creates a new Table in the database

		Arguments:
			name - table name
			temporary (default: False) - whether or not the table should be temporary
			**columns - dictionary of column names and value types (or a list of [type, default value])
				{column_name: value_type, other_column: [second_type, default_value], ...}

		Usage:
			db.createTable("users", id = "INT", username =  ["VARCHAR(50)", "user"])

		returns a Table object'''
		query = SQLString.createTable(name, temporary, **columns)
		db.execute(query)
		return db.table(name, False)

	def drop(self):
		'''Drops the table from the database

		Arguments:
			name - name of the table to be dropped

		Usage:
			db.dropTable("users")

		returns an ExecutionCursor object'''
		query = SQLString.dropTable(self.name)
		return self.execute(query)

	def rename(self, name):
		'''Renames the current table

		Arguments:
			name - new name for the table

		Usage:
			db.table("users").rename("old_users")
			
		returns an ExecutionCursor object'''
		query = SQLString.rename(self.name, name)
		return self.execute(query)

	def addColumns(self, **columns):
		'''Adds columns to the table

		Arguments:
			**columns - dictionary of column names and value types (or a list of [type, default value])
				{column_name: value_type, other_column: [second_type, default_value], ...}

		Usage:
			db.table("users").addColumns(user_type = "VARCHAR(255)", user_tier = ["INT", 0])

		returns an ExecutionCursor object'''
		add_trans = self.db.transaction()
		for column, value in columns.items():
			query = SQLString.addColumn(self.name, column, value)
			add_trans.execute(query)
		return add_trans.commit()

	def dropColumns(self, *columns): # also need to copy indices and other metadata
		'''Drops columns from the table

		Arguments:
			*columns - list of columns to drop

		Usage:
			db.table("users").dropColumns("user_type", "user_tier")

		returns an ExecutionCursor object'''
		drop_trans = self.db.transaction()
		temp_name = self.name + str(int(time.time()))
		keep_columns = list(set(self.columns()).difference(columns))
		column_str = ", ".join(keep_columns)
		drop_trans.execute("CREATE TABLE {name} ({columns})".format(name = temp_name, columns = column_str))
		drop_trans.execute("INSERT INTO {new_table} SELECT {columns} FROM {old_table}".format(new_table = temp_name, old_table = self.name, columns = column_str))
		self.drop()
		self.db.table(temp_name).rename(self.name)
		return drop_trans.commit()

	def renameColumns(self, **columns): # also need to copy indices and other metadata
		'''Renames columns in the table

		Arguments:
			**columns - dictionary of names and new names

		Usage:
			db.table("users").renameColumns(id = "user_id")

		returns an ExecutionCursor object'''
		ren_trans = self.db.transaction()
		temp_name = self.name + str(int(time.time()))
		column_names = list(columns.items())
		old_names, new_names = SQLString.extract(column_names), SQLString.extract(column_names, 1)
		current_columns = self.columns()
		keep_columns = list(set(current_columns).difference(old_names))
		new_columns = keep_columns + new_names
		ren_trans.execute("CREATE TABLE {name} ({columns})".format(name = temp_name, columns = ', '.join(new_columns)))
		ren_trans.execute("INSERT INTO {new_table} SELECT {columns} FROM {old_table}".format(new_table = temp_name, old_table = self.name, columns = ', '.join(current_columns)))
		self.drop()
		self.db.table(temp_name).rename(self.name)
		return ren_trans.commit()

	def insert(self, **columns):
		'''Inserts rows into the table

		see Database.insert for further reference'''
		return self.db.insert(self.name, **columns)

	def update(self, equal = None, like = None, where = "1 = 1", **columns):
		'''Updates rows in the table

		see Database.update for further reference'''
		return self.db.update(self.name, equal, like, where, **columns)

	def select(self, **options):
		'''Selects rows from the table

		see Database.select for further reference'''
		return self.db.select(self.name, **options)

	def delete(self, **options):
		'''Deletes rows from the table

		see Database.delete for further reference'''
		return self.db.delete(self.name, **options)

class ExecutionCursor(object):
	'''Provides additional functionality to the ExecutionCursor object'''
	def __init__(self, cursor):
		self.cursor = cursor
		self.fetchall, self.fetchone, self.description = self.cursor.fetchall, self.cursor.fetchone, self.cursor.description
		self.fetched = None

	def fetch(self, type_fetch = "all", type = dict):
		'''Fetches columns from the cursor

		Arguments:
			type_fetch - how many to fetch (all, one)
			type - type of fetch (dict, list)

		Usage:
			users = db.select("users").fetch()
			
		returns query results in specified format'''
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
			db.select("users").export("mytable.csv")

		returns None'''
		with open(filepath, 'wb') as csv_file:
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow([header[0] for header in self.description])
			csv_writer.writerows(self.cursor)

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

class SQLString(object):
	'''Internal class --- provides SQL string creation functions'''
	@classmethod
	def pragma(cls, cmd):
		'''Generates a PRAGMA SQL query

		See Database.pragma for further reference'''
		query = "PRAGMA {cmd}".format(cmd = cmd)
		return query

	@classmethod
	def checkIntegrity(cls, errors):
		'''Generates a PRAGMA INTEGRITY_CHECK query

		See Database.checkIntegrity for further reference'''
		query = cls.pragma("INTEGRITY_CHECK({errors})".format(errors = errors))
		return query

	@classmethod
	def createTable(cls, name, temporary, **columns):
		'''Generates a CREATE TABLE query

		See Database.createTable for further reference'''
		for column, value in columns.items():
			if isinstance(value, list):
				columns[column] = "{type} DEFAULT {default}".format(type = value[0], default = value[1])
			else:
				columns[column] = "{type}{other}".format(type = value, other = " NOT NULL" if value.lower() != "null" else "")
		column_str = ','.join(column + ' ' + columns[column] for column in columns)
		query = "CREATE {temp}TABLE {table} ({columns})".format(temp = "TEMPORARY " if temporary else "", table = name, columns = column_str)
		return query

	@classmethod
	def dropTable(cls, name):
		'''Generates a DROP TABLE query

		See Database.dropTable for further reference'''
		query = "DROP TABLE {table}".format(table = name)
		return query

	@classmethod
	def rename(cls, name, new_name):
		'''Generates an ALTER TABLE RENAME SQL query

		See Table.rename for further reference'''
		query = "ALTER TABLE {table} RENAME TO {name}".format(table = name, name = new_name)
		return query

	@classmethod
	def addColumn(cls, table, name, col_type):
		'''Generates an ALTER TABLE ADD SQL query

		see Table.addColumns for further reference'''
		if isinstance(col_type, list):
			column_str = "{type} DEFAULT {default}".format(type = col_type[0], default = col_type[1])
		else:
			column_str = "{type}{other}".format(type = col_type, other = " NOT NULL" if col_type.lower() != "null" else "")
		query = "ALTER TABLE {table} ADD COLUMN {name} {type}".format(table = table, name = name, type = column_str)
		return query

	@classmethod
	def insert(cls, table, **columns):
		'''Generates an INSERT SQL query

		see Database.insert for further reference'''
		column_values = list(columns.items())
		column_names = ', '.join(map(cls.escapeColumn, cls.extract(column_values)))
		values = cls.extract(column_values, 1)
		value_string = ', '.join("?" * len(values))
		query = "INSERT INTO {table} ({columns}) VALUES ({values})".format(table = table, columns = column_names, values = value_string)
		return query, values

	@classmethod
	def update(cls, table, equal = None, like = None, where = "1 = 1", **columns):
		'''Generates an UPDATE SQL query

		see Database.update for further reference'''
		column_values = list(columns.items())
		column_str = cls.joinOperatorExpressions(cls.extract(column_values), ',')
		like_str, equal_str, values_like_equal = cls.inputToQueryString(like, equal)
		values = cls.extract(column_values, 1) + values_like_equal
		where = cls.joinClauses(like_str, equal_str, where)
		query = "UPDATE {table} SET {columns} WHERE {where}".format(table = table, columns = column_str, where = where)
		return query, values

	@classmethod
	def select(cls, table, **options):
		'''Generates a SELECT SQL query

		see Database.select for further reference'''
		user_columns = options.get('columns')
		if user_columns:
			columns = ','.join('`{column}`'.format(column = column) for column in user_columns)
		else:
			columns = ALL
		like_str, equal_str, values = cls.inputToQueryString(options.get('like'), options.get('equal'))
		where = cls.joinClauses(like_str, equal_str, options.get('where', '1 = 1'))
		query = "SELECT {columns} FROM {table} WHERE {where}".format(columns = columns, table = table, where = where)
		return query, values

	@classmethod
	def delete(cls, table, **options):
		'''Generates a DELETE SQL query

		see Database.delete for further reference'''
		like_str, equal_str, values = cls.inputToQueryString(options.get('like'), options.get('equal'))
		where = cls.joinClauses(like_str, equal_str, options.get('where', '1 = 0'))
		query = "DELETE FROM {table} WHERE {where}".format(table = table, where = where)
		return query, values

	@classmethod
	def extract(cls, values, index = 0):
		'''Extracts the index value from each value

		Arguments:
			values - list of lists to extract from
			index - index to select from each sublist (optional, defaults to 0)

		Usage:
			columns = extract([["a", 1], ["b", 2]]) # ["a", "b"]
			values  = extract([["a", 1], ["b", 2]], 1) # [1, 2]

		returns list of selected elements'''
		return map(lambda item: item[index], values)

	@classmethod
	def escapeString(cls, value):
		'''Escapes a string

		Arguments:
			value - value to escape

		Usage:
			escaped_value = escapeString("value") # 'value'

		returns escaped string'''
		return "'{string}'".format(string = value) if isinstance(value, basestring) else value

	@classmethod
	def escapeColumn(cls, value):
		'''Escapes a column name

		Arguments:
			value - column name to escape

		Usage:
			escaped_column = escapeColumn("time") # `time`

		returns escaped column name'''
		return "`{column}`".format(column = value)

	@classmethod
	def joinExpressions(cls, exps, operator, func = lambda item: item):
		'''Joins numerous expressions with an operator

		Arguments:
			exps - iterable list of expressions to join
			operator - operator to use in joining expressions (OR, AND, LIKE, etc)
			func - optional function to call on each expression before joining

		Usage:
			joined_expr = joinExpressions(["date = NOW", "id = 1"], "OR") # date = NOW OR id = 1
			joined_expr = joinExpressions(["date = NOW", "id = 1"], "OR", escapeColumn) # `date` = NOW OR `id` = 1

		returns joined expressions as one string'''
		new_op = " {op} ".format(op = operator)
		return new_op.join(func(exp) for exp in exps)

	@classmethod
	def joinOperatorExpressions(cls, exps, operator, second_operator = "=", value = "?"):
		'''Joins numerous expressions with two operators (see below)

		Arguments:
			exps - iterable list of expressions to join
			operator - operator to use in joining expressions (OR, AND, LIKE, etc)
			second_operator - operator to join each expression and value (defaults to =)
			value - what to be used as a value with the second_operator (defaults to ?)

		Usage:
			joined_expr = joinOperatorExpressions(["date", "now"], "OR") # `date` = ? OR `now` = ?
			joined_expr = joinOperatorExpressions(["date", "now"], "OR", "LIKE", "'%'") # `date` LIKE '%' OR `now` LIKE '%'

		returns joined expressions as one string'''
		func = lambda item: "{column} {operator} {exp}".format(column = cls.escapeColumn(item) , operator = second_operator, exp = value)
		return cls.joinExpressions(exps, operator, func)

	@classmethod
	def joinClauses(cls, *clauses):
		'''Joins numerous clauses with the AND operator

		Arguments:
			*clauses -list of clauses to join 

		Usage:
			clause = joinClauses('`user` LIKE 'name%', '`id` = 5')

		returns joined clauses'''
		return ' AND '.join(filter(lambda item: bool(item), clauses))

	@classmethod
	def inputToQueryString(cls, like, equal):
		'''Internal function --- converts user input to an SQL string

		Arguments;
			like - dictionary of like values
			equal - dictionary of equal values

		Usage:
			like_str, equal_str, values = inputToQueryString(like, equal)

		returns SQL like string, SQL equal string, and formatted values'''
		if not like:
			like = {}
		if not equal:
			equal = {}
		like_items, equal_items = list(like.items()), list(equal.items())
		like_str = cls.joinOperatorExpressions(cls.extract(like_items), 'AND', "LIKE")
		equal_str = cls.joinOperatorExpressions(cls.extract(equal_items), "AND")
		values = cls.extract(like_items, 1) + cls.extract(equal_items, 1)
		return like_str, equal_str, values
