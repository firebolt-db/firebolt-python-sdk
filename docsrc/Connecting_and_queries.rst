.. _examples:

###############################
Connecting and running queries
###############################

This topic provides a walkthrough and examples for how to use the Firebolt Python SDK to connect to Firebolt resources to run commands and query data. 


Importing required modules
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Firebolt Python SDK requires you to import the following modules before making any command or query requests to your Firebolt database. 

.. _required_connection_imports:

:: 

	from firebolt.db import connect
	from firebolt.client import DEFAULT_API_URL


.. _connecting_with_credentials_example:

Connecting to your database / engine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the Python SDK to execute commands and queries, you must provide the following account information through a connection request. 

+------------------------------------+-------------------------------------------------------------------+
| ``username``                       |  The email address associated with your Firebolt user.            |
+------------------------------------+-------------------------------------------------------------------+
| ``password``                       |  The password used for connecting to Firebolt.                    |
+------------------------------------+-------------------------------------------------------------------+
| ``database``                       |  The name of the database you would like to connect to.           |
+------------------------------------+-------------------------------------------------------------------+
| ``engine_name`` or ``engine_url``  |  The name or URL of the engine to use for SQL queries.            |
+------------------------------------+-------------------------------------------------------------------+

This information can be provided in multiple ways.

* **Set credentials manually**

	You can manually include your account information in a connection object in your code for any queries you want to request. 

	Replace the values in the example code below with your Firebolt account credentials as appropriate. 

	::

		username = "your_username"
		password = "your_password"
		engine_name = "your_engine"
		database_name = "your_database"

		connection = connect( 
			engine_name=engine_name,
			database=database_name,
			username=username,
			password=password,
		)
		
		cursor = connection.cursor()


* **Use an .env file**

	Consolidating all of your Firebolt credentials into a ``.env`` file can help protect sensitive information from exposure. Create an ``.env`` file with the following key-value pairs, and replace the values with your information. 

	::

		FIREBOLT_USER="your_username"
		FIREBOLT_PASSWORD="your_password"
		FIREBOLT_ENGINE="your_engine"
		FIREBOLT_DB="your_database"

	Be sure to place this ``.env`` file into your root directory. 

	Your connection script can load these environmental variables from the ``.env`` file by using the `python-dotenv <https://pypi.org/project/python-dotenv/>`_ package. Note that the example below requires additional imports. 

	::

		import os
		from dotenv import load_dotenv

		load_dotenv()

		connection = connect(
			username=os.getenv('FIREBOLT_USER'),
			password=os.getenv('FIREBOLT_PASSWORD'),
			engine_name=os.getenv('FIREBOLT_ENGINE'),
			database=os.getenv('FIREBOLT_DB')
		)

		cursor = connection.cursor()


Executing commands and queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _basic_execute_example:

The ``cursor`` object created in the prior script can be used to run various SQL commands and queries on a database. The example below uses ``cursor`` to create a new table ``test_table``, inserts rows into it, and then selects the table's contents. 

The engine attached to your specified database must be started before executing any queries. For help, see :ref:`starting an engine` 

::

	cursor.execute(
    		'''CREATE FACT TABLE IF NOT EXISTS test_table (
    			id INT, 
    			name TEXT 
    			) 
    			PRIMARY INDEX id;'''
		)
	
	cursor.execute(
    		'''INSERT INTO test_table VALUES 
    			(1, 'hello'),
    			(2, 'world'),
    			(3, '!');'''
		)

	cursor.execute(
			'''SELECT * FROM test_table;'''
		)

	cursor.close()

.. note:: 

	For reference documentation on ``cursor`` functions, see :ref:`Db.cursor` 


Fetching query results
^^^^^^^^^^^^^^^^^^^^^^^

After running a query, you can fetch the results using a ``cursor`` object. The examples below use the data queried from ``test_table`` created in the :ref:`execute example <basic_execute_example>`. 

.. _fetch_example:

::

	print(cursor.fetchone())

**Returns**: ``[2, 'world']``		

::

	print(cursor.fetchmany(2))

**Returns**: ``[[1, 'hello'], [3, '!']]``

::

	print(cursor.fetchall())

**Returns**: ``[[2, 'world'], [1, 'hello'], [3, '!']]``


Executing parameterized queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _parameterized_query_execute_example:

Parameterized queries (also known as “prepared statements”) format a SQL query with placeholders and then pass values into those placeholders when the query is run. This protects against SQL injection attacks and also helps manage dynamic queries that are likely to change, such as filter UIs or access control. 

To run a parameterized query, use the ``execute()`` cursor method. Add placeholders to your statement using question marks ``?``, and in the second argument pass a tuple of parameters equal in length to the  number of ``?`` in the statement.


:: 

	cursor.execute(
		'''CREATE FACT TABLE IF NOT EXISTS test_table2 (
			id INT,
			name TEXT, 
			date_value DATE
		)
			PRIMARY INDEX id;'''
		)


::
	
	cursor.execute(
		"INSERT INTO test_table2 VALUES (?, ?, ?)",
			(1, "apple", "2018-01-01"),
		)

	cursor.close()

.. _parameterized_query_executemany_example:

If you need to run the same statement multiple times with different parameter inputs, you can use the ``executemany()`` cursor method. This allows multiple tuples to be passed as values in the second argument.

::

	cursor.executemany(
		"INSERT INTO test_table2 VALUES (?, ?, ?)",
		(
			(2, "banana", "2019-01-01"), 
			(3, "carrot", "2020-01-01"), 
			(4, "donut", "2021-01-01")
		)
	)

	cursor.close()



Executing multiple-statement queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Multiple-statement queries allow you to run a series of SQL statements sequentially with just one method call. Statements are separated using a semicolon ``;``, similar to making SQL statements in the Firebolt UI.

:: 

	cursor.execute(
		"""
			SELECT * FROM test_table WHERE id < 4;
			SELECT * FROM test_table WHERE id > 2;
		"""
		)
	print("First query: ", cursor.fetchall())
	assert cursor.nextset()
	print("Second query: ", cursor.fetchall())
	assert cursor.nextset() is None

	cursor.close()

**Returns**: 

:: 

	First query:  [[2, 'banana', datetime.date(2019, 1, 1)], [3, 'carrot', datetime.date(2020, 1, 1)], [1, 'apple', datetime.date(2018, 1, 1)]]
	Second query:  [[3, 'carrot', datetime.date(2020, 1, 1)], [4, 'donut', datetime.date(2021, 1, 1)]]


Using DATE and DATETIME values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

DATE, DATETIME and TIMESTAMP values used in SQL insertion statements must be provided in a specific format, otherwise they could be read incorrectly. 

* DATE values should be formatted as **YYYY-MM-DD** 

* DATETIME and TIMESTAMP values should be formatted as **YYYY-MM-DD HH:MM:SS.SSSSSS**

The `datetime <https://docs.python.org/3/library/datetime.html>`_ module from the Python standard library contains various classes and methods to format DATE, TIMESTAMP and DATETIME data types. 

You can import this module as follows.  

:: 

	from datetime import datetime

