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

### Executing Commands

Commands/queries can easily be sent to the database:

```python

query = db.execute("SELECT * FROM myTable")
```

Alternatively, the Database.query function can be used:

```python

query = db.query("SELECT * FROM myTable")
```

Any command or query will return a [sqlite3.Cursor](https://docs.python.org/2/library/sqlite3.html?highlight=sqlite3#cursor-objects "Python Documentation: sqlite3.Cursor") instance.
