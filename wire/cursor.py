# Rushy Panchal
# wire/cursor.py
# Provides the ExecutionCursor and Transaction classes

import csv

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
