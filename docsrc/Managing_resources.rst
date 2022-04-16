#####################################
Managing engines and databases
#####################################

This topic provides a walkthrough and examples for using the Firebolt Python SDK to create and modify Firebolt databases and engines.  


Importing modules
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

