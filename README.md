PySQLite
========

Python Wrapper for the standard sqlite3 API

PySQLite serves to simplify the process of creating and maintaining local databases.

It utilizes the built-in sqlite3 package, and does not require any external files.
Because of this, it is extremely lightweight.

Examples
------------

### Creating/Accessing a database

```python
import pysqlite
# the import is assumed for the rest of the examples

db = pysqlite.Database("test.db")
# the database is created if it does not exist already
```

Alternatively, a database can be created by executing a script of SQL commands:

```python

db = pysqlite.Database.create("test_creator.sql")
```

### Creating or Dropping Tables

*Database.createTable* and *Database.dropTable* can be used to create or drop tables, respectively.

```python

db.createTable("users", id = "INT", username = ["VARCHAR(50)", "default_name"])
# creating a table

db.dropTable("users")
# drops the table
```

### Default Table

Instead of providing the table name for every function call, a default table can be set with the *Database.setTable* function. Every subsequent function call will use this table, unless a different table is provided:

```python
db.setTable("users")
```

### Executing Commands

Commands/queries can easily be sent to the database:

```python

query = db.execute("SELECT * FROM myTable")
```

Alternatively, the *Database.query* function can be used:

```python

query = db.query("SELECT * FROM myTable")
```

Any command or query will return a [sqlite3.Cursor](https://docs.python.org/2/library/sqlite3.html?highlight=sqlite3#cursor-objects "Python Documentation: sqlite3.Cursor") instance.

Common queries, such as insert, update, select, and delete, are built in.

### Inserting Rows

Rows can be inserted into the database by using the *Database.insert* function:

```python
db.insert("users", id = 5, username = "panchr")
```

### Updating, Selecting, and Deleting Rows: Clauses

Whenever you want to update, select, or delete a row, you have to specify a clause.
This clause searches for specific rows. If a clause is not provided, the query will be executed on **every** row (it defaults to *1 = 1*).

Currently, PYSQLite supports three types of clauses (combinations among the three are allowed): *WHERE*, *LIKE*, and a mix.

These can be provided as arguments. To any function that requires a row filtering method, the following arguments are availabile:

* equal: this is a dictionary of columns and values to use in the *WHERE* clause using the *=* operator
* like: also a dictionary of columns and values, but it utilizes the *LIKE* operator

Both *equal* and *like* use the *AND* operator to join clauses.

* where: This is a custom SQL clause. If you want to use an *OR* operator, this must be used.

For example, you can select all of the rows that have an ID of 5, using *Database.select*:

```python
db.select("users", columns = ALL, equal = {"id": 5})
```

Alternatively, you can update every row to have an ID of 6, whose "username" column starts with "pan", using *Database.update*:

```python
db.update("users", like = {"username": "pan%"}, id = 6)
```

Finally, deleting a row whose username starts with "pan" or has an ID of 5, using *Database.delete*:

```python
db.delete("users", where = "`id`=5 OR `username` LIKE 'pan%'")
```
