#####################################
Managing engines and databases
#####################################

This topic provides a walkthrough and examples for using the Firebolt Python SDK to create and modify Firebolt databases and engines.  


Setting up a ResourceManager object
====================================

You can perform various functions on Firebolt databases and engines by calling a ``ResourceManager`` object, which must be configured with its own user credentials through the imported ``Settings`` class. 

To get started, follow the steps below: 

**1. Import modules**

	To initialize a ``ResourceManager`` object, import the modules shown below. 

.. _required_resourcemanager_imports:

	:: 

		from firebolt.service.manager import ResourceManager
		from firebolt.common import Settings


**2. Initialize a Settings object**

	A Settings object contains the user credentials and other information needed to manage Firebolt databases and engines.   

	The Settings object uses the following parameters: 

	+---------------------+-----------------------------------------------------------------------------------------------------------------------------+
	| ``user``            |  The email address associated with your Firebolt user profile.                                                              |
	+---------------------+-----------------------------------------------------------------------------------------------------------------------------+
	| ``password``        |  The password used for connecting to Firebolt.                                                                              |
	+---------------------+-----------------------------------------------------------------------------------------------------------------------------+
	| ``server``          |  The API hostname for logging in. Defaults to ``api.app.firebolt.io`` if not included.                                      |
	+---------------------+-----------------------------------------------------------------------------------------------------------------------------+
	| ``default_region``  |  The default region for creating new databases and engines.                                                                 |
	|                     |                                                                                                                             |
	|                     |  For more information, see `Available AWS Regions <https://docs.firebolt.io/general-reference/available-regions.html>`_.    |
	+---------------------+-----------------------------------------------------------------------------------------------------------------------------+



	A ``Settings`` object can be configured with parameters by multiple methods.  

		* Add the parameters manually in your command script: 

			:: 

				settings = Settings(
					user="your_username",
					password="your_password",
					server="api.app.firebolt.io"
					default_region="your_region"
					)

		* Use a ``.env`` file located in your root directory containing the following parameters: 

			:: 

				FIREBOLT_USER="your_username",
				FIREBOLT_PASSWORD="your_password",
				FIREBOLT_SERVER="api.app.firebolt.io"
				FIREBOLT_DEFAULT_REGION="your_region"

			In your application file, the ``Settings`` object can read the values from the ``.env`` file if it is set to ``None`` instead of having values, as shown below: 

			:: 

				settings = None


**3. Initialize a ResourceManager object**


	After the ``Settings`` are configured, create a ``ResourceManager`` object, which is given the variable name ``rm`` in the example below. 

		:: 

			rm = ResourceManager(settings=settings)

	.. note:: 
	
		Subsequent examples on this page use the ``rm`` object for database and engine functions. 


Database function examples
====================================

This section includes Python examples of various common functions for creating and managing Firebolt resources. 

Listing out databases 
------------------------

List out the names of all databases under your account by using the ``get_many`` function.  



	**List out all databases and their metadata** 

		This produces an inventory of all databases and their metadata from your account. The Python `devtools <https://pypi.org/project/devtools/>`_ module used in the example below helps format the metadata to be more readable.  

		::

			from devtools import debug

			debug(rm.databases.get_many())


	**Listing out databases by name**

		This function call lists out the names of your databases, but it can be modified to list out other attributes. This is helpful for tracking down a particular database in your account. 

		:: 

			all_dbs = rm.databases.get_many()
			all_db_names = [d.name for d in all_dbs]
			for db in db_names: 
				print(db)

	.. note::

		For a list of all database attributes, see :ref:`model.database`. 


Creating a new database
-------------------------

Launch a new database and use it to create a ``database`` object. 

A newly created database uses the default region from your Settings unless you specify a different region as a parameter. 

	::

		database = rm.databases.create(name="database_name", region="us-east-1")


	.. note:: 

		For a list of all database parameters, see :ref:`Service.database` 


Locating a database
---------------------

Find a specific Firebolt database by using its name or ID. These functions are useful as a starting point to create a ``database`` object that can be called in other database functions.  

In the examples below, replace the values for ``database_name`` and ``database_id`` with your database name or ID. 



	**Locating by name**

		:: 

			database = rm.databases.get_by_name(name="database_name")

	**Locating by ID**

		::

			database = rm.databases.get_by_id(id="database_id")


Getting database status
-------------------------

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to format metadata from a ``database`` object. This is a helpful command to run after a database operation to check if its execution was successful.    

	::	
	
		from devtools import debug
		debug(database)


Dropping a database
-----------------------

Delete a database by calling the ``delete`` function. The database is deleted along with all of its tables.

	:: 
	
		database.delete()


Engine function examples
====================================

This section includes Python examples of various common functions for creating and managing Firebolt engines. 



Creating an engine
--------------------

Launch a new Firebolt engine and create an ``engine`` object. The created engine uses the default region included in your Settings unless you specify a different region as a parameter. 

	:: 

		engine = rm.engines.create(name="engine_name")


.. note:: 

	For a list of all engine parameters, see :ref:`Service.engine` 



Listing out engines
---------------------

List out the names of all engines under your account by using the ``get_many`` function.

	**List out all engines and metadata**

		This produces an inventory of all engines and their metadata from your account. The Python `devtools <https://pypi.org/project/devtools/>`_ module used in the example below helps format the metadata to be more readable.  

		::

			from devtools import debug

			debug(rm.engines.get_many())

	**List out engines by name**

		This function call lists out the names of your engines, but it can be modified to list out other attributes. This is helpful for tracking down a particular engine in your account. 

		::

			all_engines = rm.engines.get_many()
			all_engine_names = [e.name for e in all_engines]
			for name in all_engine_names: 
				print(name)


	.. note:: 

		For a list of all engine attributes, see :ref:`Model.engine`

Locating an engine
--------------------

Find a specific Firebolt engine by using its name or ID. These functions are useful as a starting point to create an ``engine`` object that can be called in other engine functions.

In the examples below, replace the values for ``engine_name`` and ``engine_id`` with your engine name or ID. 

	**Locating by name**

		::

			engine = rm.engines.get_by_name(name="engine_name")

	**Locating by ID**

		::

			engine = rm.engines.get_by_id(name="engine_id")


Attaching an engine
---------------------

Attach an engine to a database. An engine must be attached to a database and started before it can run SQL commands or queries. 

	:: 

		engine = rm.engines.get_by_name(name="engine_name")
		engine.attach_to_database(
			database=rm.databases.get_by_name(name="database_name"))



Dropping an engine
--------------------

Delete an engine by calling the ``delete`` function. The engine is removed from its attached database and deleted. 

	::

		engine.delete()


Starting an engine
-------------------

Start an engine by calling the ``start`` function on an ``engine`` object. An engine must be attached to a database and started before it can run SQL commands or queries. 

	::

		engine.start() 



Stopping an engine
--------------------

Stop an engine by calling the ``stop`` function. When stopped, an engine is not available to run queries and does not accrue additional usage time on your account. 

	::

		engine.stop()

Updating an engine
---------------------

Update an engine to change its specifications, returning an updated version of the engine. The engine must be stopped in order to be updated. 

For a list of engine parameters that can be updated, see :meth:`~firebolt.model.engine.Engine.update`

	::

		engine.update(description = "This is a new description.")

Getting engine status
----------------------

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to format metadata from an ``engine`` object. This is a helpful command to run after an engine operation to check if its execution was successful.    

	::	
	
		from devtools import debug
		debug(engine)

