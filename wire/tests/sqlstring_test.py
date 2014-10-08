
import unittest
import wire

class TestSQLString(unittest.TestCase):
	def setUp(self):
		'''Sets up the test case'''
		self.sql = wire.SQLString

	def test_pragma(self):
		'''Tests the PRAGMA SQL generation'''
		self.assertEqual(self.sql.pragma("INTEGRITY_CHECK(10)"), "PRAGMA INTEGRITY_CHECK(10)")
		self.assertEqual(self.sql.checkIntegrity(5), "PRAGMA INTEGRITY_CHECK(5)")

	def test_createTable(self):
		'''Tests the CREATE TABLE SQL generation'''
		table_outputs = ["CREATE TABLE test (id INT NOT NULL,username VARCHAR(255) DEFAULT 'default_user')",
			"CREATE TABLE test (username VARCHAR(255) DEFAULT 'default_user',id INT NOT NULL)"]
		temp_table_outputs = ["CREATE TEMPORARY TABLE test_temp (value REAL DEFAULT 0.0,time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			"CREATE TEMPORARY TABLE test_temp (time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,value REAL DEFAULT 0.0)"]
		self.assertIn(self.sql.createTable("test", False, id = "INT", username = ["VARCHAR(255)", "'default_user'"]), table_outputs)
		self.assertIn(self.sql.createTable("test_temp", True, value = ["REAL", 0.0], time = ["TIMESTAMP", "CURRENT_TIMESTAMP"]), temp_table_outputs)
		# include a Temp table test (False --> True)

	def test_dropTable(self):
		'''Tests the DROP TABLE SQL generation'''
		self.assertEqual(self.sql.dropTable("table_drop"), "DROP TABLE table_drop")
		self.assertEqual(self.sql.dropTable("some_other_table"), "DROP TABLE some_other_table")

	def test_renameTable(self):
		'''Tests the ALTER TABLE RENAME SQL generation'''
		self.assertEqual(self.sql.rename("orig_table", "new_table"), "ALTER TABLE orig_table RENAME TO new_table")

if __name__ == '__main__':
	unittest.main()