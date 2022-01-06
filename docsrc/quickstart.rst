########################
Quickstart
########################

========================
Installation
========================

*  Before installation, you first must install Python version 3.7 or later. 

*  The Firebolt SDK can be installed via **pip**: ``pip install firebolt-sdk`` 

==========================
Connection parameters
==========================

These parameters are used to connect to a Firebolt database:

* **engine_name** - The name of the Firebolt engine to process requests. You can find the names of your engines from the Firebolt SQL workspace. **engine_name** is unnecessary if you specify **engine_url** instead.    
* **engine_url** - The URL of a Firebolt engine to process requests. You can retrieve the engine URL from the `engine <https://github.com/firebolt-db/firebolt-sdk/tree/main/src/firebolt/model/engine.py>`_ attribute endpoint or through other methods described in our `documentation <https://docs.firebolt.io/developing-with-firebolt/firebolt-rest-api.html#get-the-url-of-an-engine>`_. **engine_url** is unnecessary if you specify **engine_name** instead.    
* **database** - The name of the Firebolt database you want to access
* **username** - Your Firebolt account username
* **password** - Your Firebolt account password
* **api_endpoint** - (*optional*) The API hostname for logging in. Defaults to ``api.app.firebolt.io``.

==========================
Examples
==========================

--------------------------------
Importing required modules
--------------------------------

These modules are necessary before making any requests to your Firebolt database 

:: 

	from firebolt.db import connect
	from firebolt.client import DEFAULT_API_URL

--------------------------------------------------
Connecting with credentials and creating a cursor
--------------------------------------------------

The script below sends your credentials to connect to your database and then creates a cursor object for any queries you want to request. 

::

	engine_name = "<my_engine>"
	database_name = "<my_database>"
	username = "<my_username>"
	password = "<my_password>"

	connection = connect( 
    		engine_name=engine_name,
    		database=database_name,
    		username=username,
    		password=password,
		)
	cursor = connection.cursor()

----------------------------------------
Executing a database query
----------------------------------------

This code below uses the ``cursor`` object created in the prior script to run various SQL queries on a database. It create a new table ``test_table``, insert rows into it, and then selects the table's contents. 



::

	cursor.execute(
    		'''CREATE FACT TABLE IF NOT EXISTS test_table (
    			id int, 
    			name text, 
    			dt datetime
    		) 
    			primary index id'''
	)
	
	cursor.execute(
    		'''INSERT INTO test_table VALUES 
    			(1, 'hello', '2021-01-01 01:01:01'),
    			(2, 'world', '2022-02-02 02:02:02'),
    			(3, '!', '2023-03-03 03:03:03')'''
	)

	cursor.execute(
			'''SELECT * FROM test_table'''
	)

.. note:: 

   Your database engine must be started before executing any queries. 

----------------------------------------
Fetching query results
----------------------------------------

After making a query, you can fetch the results from the same ``cursor`` object. 

::

		print(cursor.fetchall())

**Returns**: 

:: 

	[[2, 'world', datetime.datetime(2022, 2, 2, 2, 2, 2)], [1, 'hello', datetime.datetime(2021, 1, 1, 1, 1, 1)], [3, '!', datetime.datetime(2023, 3, 3, 3, 3, 3)]

For further examples, please visit our `Github repository <https://github.com/firebolt-db/firebolt-sdk/tree/main/examples/dbapi.ipynb>`_.

See `PEP-249 <https://www.python.org/dev/peps/pep-0249>`_ for further information on Python database API references and specifications. 




==========================
Optional features 
==========================

By default, the Firebolt Python SDK uses the ``datetime`` module to parse date and datetime values, but this might be slow for large operations. In order to speed up datetime operations, its possible to use `ciso8601 <https://pypi.org/project/ciso8601/>`_ package. 

To install firebolt-python-sdk with ``ciso8601`` support, run ``pip install firebolt-sdk[ciso8601]``