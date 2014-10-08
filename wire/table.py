# Rushy Panchal
# wire/table.py
# The Table class provides an interface for direct Table manipulation

from sqlstring import SQLString
from cursor import ExecutionCursor

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
		