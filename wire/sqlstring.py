# Rushy Panchal
# wire/sqlstring.py
# The SQL String class generates SQL queries

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