.. _examples:

########################
Usage examples
########################

This topic provides a series of examples for how to use the Firebolt Python SDK to perform standard operations such as connecting, querying data, and managing resources. 


--------------------------------------------------
Connecting and running queries
--------------------------------------------------

The Firebolt Python SDK requires you to import the following modules before making any command or query requests to your Firebolt database. 

.. _required_connection_imports:

:: 

	from firebolt.db import connect
	from firebolt.client import DEFAULT_API_URL




.. _connecting_with_credentials_example:

Connecting to your database / engine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To access your Firebolt account, you must first provide your account information through a connection request. This information can be provided in multiple ways.


* **Set credentials manually**

	You can manually include your account information in a connection object in your application for any queries you want to request. 

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

	Consolidating all of your Firebolt credentials into a ``.env`` file can help protect sensitive information from exposure. Replace the values with your information in the example ``.env`` file below. 

	::

		FIREBOLT_USER="your_username"
		FIREBOLT_PASSWORD="your_password"
		FIREBOLT_ENGINE="your_engine"
		FIREBOLT_DB="your_database"

	Be sure to place this ``.env`` file into your root directory. 

	Your connection script can load these environmental variables from the ``.env`` file by using the `python-dotenv <https://pypi.org/project/python-dotenv/>`_ package. Note that this package requires additional imports. 

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

The ``cursor`` object created in the prior script can be used to run various SQL commands and queries on a database. The example below uses ``cursor`` to create a new table ``test_table``, insert rows into it, and then select the table's contents. 

The engine attached to your specified database must be started before executing any queries. For help, see :ref:`starting an engine` 

For a reference 

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


.. note:: 

	For reference documentation on ``cursor`` functions, see :ref:`Db.cursor` 


Fetching query results
^^^^^^^^^^^^^^^^^^^^^^^

After running a query, you can fetch the results using a ``cursor`` object. The examples below use ``test_table`` created in the :ref:`execute example <basic_execute_example>`. 

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


--------------------------------------------------
Working with engines and databases
--------------------------------------------------

You can perform various functions on Firebolt databases and engines by calling a ``ResourceManager`` object, which must be configured with its own user credentials through the imported ``Settings`` class. 

To initialize a ``ResourceManager`` object, you need to import the modules shown below. 

.. _required_resourcemanager_imports:

:: 

	from firebolt.service.manager import ResourceManager
	from firebolt.common import Settings


Initializing a Settings object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A Settings object contains the user credentials and other information needed to manage Firebolt databases and engines.   

The Settings object requires the following parameters: 

* ``user`` - The email address associated with your Firebolt user profile.

* ``password`` - The password used for connecting to Firebolt.   

* ``server`` - Use ``api.app.firebolt.io``.

* ``default_region`` - The default region for creating new databases and engines. For more information, see `Available AWS Regions <https://docs.firebolt.io/general-reference/available-regions.html>`_.


A ``Settings`` object can be configured with parameters by two different methods.  

* Add the parameters manually in your command script. 

	:: 

		settings = Settings(
			user="your_username",
			password="your_password",
			server="api.app.firebolt.io"
			default_region="your_region"
		)

* Use a ``.env`` file located in your root directory containing the following parameters. 

	:: 

		FIREBOLT_USER="your_username",
		FIREBOLT_PASSWORD="your_password",
		FIREBOLT_SERVER="api.app.firebolt.io"
		FIREBOLT_DEFAULT_REGION="your_region"

	In your application file, the ``Settings`` object can read the values from the ``.env`` file if it is set to ``None`` instead of having values, as shown below. 

	:: 

		settings = None


Initializing a ResourceManager object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After your ``Settings`` are configured, you can create a ``ResourceManager`` object, which is given the variable name ``rm`` in the example below. 

Subsequent examples in this section use the ``rm`` object for database and engine functions.  

:: 

	rm = ResourceManager(settings=settings)

Listing out databases 
^^^^^^^^^^^^^^^^^^^^^^^

List out the names of all databases under your account. 

:: 

	all_dbs = rm.databases.get_many()
	all_db_names = [d.name for d in all_dbs]
	print(all_db_names)


Creating a new database
^^^^^^^^^^^^^^^^^^^^^^^^

Launch a new database and use it to create a ``database`` object. 

A newly created database uses the default region from your Settings unless you specify a different region as a parameter. 

::

	database = rm.databases.create(name="database_name", region="us-east-1")

.. note:: 

	For a list of all database parameters, see :ref:`Service.database` 


Locating a database
^^^^^^^^^^^^^^^^^^^^

Find a specific Firebolt database and create a ``database`` object by using its name or ID. In the examples below, replace the values for ``database_name`` and ``database_id`` with your information. 


	**Locating by name**

		:: 

			database = rm.databases.get_by_name(name="database_name")

	**Locating by ID**

		::

			database = rm.databases.get_by_id(id="database_id")


Getting database status
^^^^^^^^^^^^^^^^^^^^^^^

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to get metadata on a ``database`` object. This is a helpful command to run after a database operation to check if its execution was successful.    

::	
	
	from devtools import debug
	debug(database)


Dropping a database
^^^^^^^^^^^^^^^^^^^^

Drop a database by calling the ``delete`` function. 

:: 
	
	database.delete()


Creating an engine
^^^^^^^^^^^^^^^^^^^

Launch a new Firebolt engine and create an ``engine`` object. The created engine uses the default region included in your settings unless you specify a different region as a parameter. 

:: 

	engine = rm.engines.create(name="engine_name")


.. note:: 

	For a list of all engine parameters, see :ref:`Service.engine` 



Listing out engines
^^^^^^^^^^^^^^^^^^^^

List out all engines affiliated with your Firebolt account. 

	**By name**

	::

		all_engines = rm.engines.get_many()
		all_engine_names = [e.name for e in all_engines]
		for name in all_engine_names: 
			print(name)


	**By ID**

	::

		all_engines = rm.engines.get_many()
		all_engine_ids = [e.engine_id for e in all_engines]
		for id in all_engine_ids: 
			print(id)

Locating an engine
^^^^^^^^^^^^^^^^^^^^

Find a specific Firebolt engine and create an ``engine`` object by using its name or ID. 

In the examples below, replace the values for ``engine_name`` and ``engine_id`` with your information. 

	**Locating by name**

		::

			engine = rm.engines.get_by_name(name="engine_name")

	**Locating by ID**

		::

			engine = rm.engines.get_by_id(name="engine_id")



Attaching an engine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Attach an engine to a database. 

An engine must be attached to a database and started before it can run SQL commands or queries. 

:: 

	engine = rm.engines.get_by_name(name="engine_name")
	engine.attach_to_database(
		database=rm.databases.get_by_name(name="database_name"))



Dropping an engine
^^^^^^^^^^^^^^^^^^^

Drop an engine by calling the ``delete`` function. 

::

	engine.delete()


Starting an engine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start an engine by calling the ``start`` function on an ``engine`` object. 

::

	engine.start() 



Stopping an engine
^^^^^^^^^^^^^^^^^^^

Stop an engine by calling the ``stop`` function. 

::

	engine.stop()

Getting engine status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to get metadata on an ``engine`` object. This is a helpful command to run after an engine operation to check if its execution was successful.    

::	
	
	from devtools import debug
	debug(engine)





