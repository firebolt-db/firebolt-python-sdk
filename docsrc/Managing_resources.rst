#####################################
Managing engines and databases
#####################################

This topic provides a walkthrough and examples for using the Firebolt Python SDK to
create and modify Firebolt databases and engines.


Setting up a ResourceManager object
====================================

You can perform various functions on Firebolt databases and engines by calling a
``ResourceManager`` object, which must be configured with its own user credentials.

To get started, follow the steps below:

**1. Import modules**

    To initialize a ``ResourceManager`` object, import the modules shown below.

.. _required_resourcemanager_imports:

    ::

        from firebolt.client import DEFAULT_API_URL
        from firebolt.client.auth import ClientCredentials
        from firebolt.service.manager import ResourceManager


**2. Initialize a ResourceManager object**

    A ResourceManager object contains the user credentials and other information needed to
    manage Firebolt databases and engines.

    ResourceManager uses the following parameters:

    +---------------------+-----------------------------------------------------------------------------------------------------------------------------+
    | ``auth``            |  Auth object, containing your credentials. See :ref:`Auth <firebolt.client:auth>` for more details.                         |
    +---------------------+-----------------------------------------------------------------------------------------------------------------------------+
    | ``api_endpoint``    |  The API hostname for logging in. Defaults to ``api.app.firebolt.io`` if not included.                                      |
    +---------------------+-----------------------------------------------------------------------------------------------------------------------------+
    | ``account_name``    |  The name of the account you're using to connect to Firebolt. Must be specified in order to authenticate.                   |
    +---------------------+-----------------------------------------------------------------------------------------------------------------------------+


    ::

        rm = ResourceManager(
            auth=ClientCredentials("your_service_account_id", "your_service_account_secret"),
            account_name="your_acc_name",
        )

    .. note::

        Subsequent examples on this page use the ``rm`` object for database and engine functions.


Database function examples
====================================

This section includes Python examples of various common functions for creating and managing
Firebolt resources.

Listing out databases
------------------------

List out the names of all databases under your account by using the ``get_many`` function.



    **List out all databases and their metadata**

        This produces an inventory of all databases and their metadata from your account.
        The Python `devtools <https://pypi.org/project/devtools/>`_ module used in the
        example below helps format the metadata to be more readable.

        ::

            from devtools import debug

            debug(rm.databases.get_many())


    **Listing out databases by name**

        This function call lists out the names of your databases, but it can be modified
        to list out other attributes. This is helpful for tracking down a particular
        database in your account.

        ::

            all_dbs = rm.databases.get_many()
            all_db_names = [d.name for d in all_dbs]
            for db in db_names:
                print(db)

    .. note::

        For a list of all database attributes, see :ref:`model-database`.


Creating a new database
-------------------------

Launch a new database and use it to create a ``database`` object.

A newly created database uses the default region from your Settings unless you specify a different region as a parameter.

    ::

        database = rm.databases.create(name="database_name", region="us-east-1")


    .. note::

        For a list of all database parameters, see :ref:`service-database`


Locating a database
---------------------

Find a specific Firebolt database by using its name. This function is useful as
a starting point to create a ``database`` object that can be called in other database functions.

In the example below, replace the values for ``database_name`` with your database name.


    **Locating by name**

        ::

            database = rm.databases.get("database_name")


Getting database status
-------------------------

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to format metadata
from a ``database`` object. This is a helpful command to run after a database operation to
check if its execution was successful.

    ::

        from devtools import debug
        debug(database)


Dropping a database
-----------------------

Delete a database by calling the ``delete`` function. The database is deleted along with
all of its tables.

    ::

        database.delete()


Engine function examples
====================================

This section includes Python examples of various common functions for creating and managing
Firebolt engines.



Creating an engine
--------------------

Launch a new Firebolt engine and create an ``engine`` object. The created engine uses the
default region included in your Settings unless you specify a different region as a parameter.

    ::

        engine = rm.engines.create("engine_name")


.. note::

    For a list of all engine parameters, see :ref:`service-engine`



Listing out engines
---------------------

List out the names of all engines under your account by using the ``get_many`` function.

    **List out all engines and metadata**

        This produces an inventory of all engines and their metadata from your account.
        The Python `devtools <https://pypi.org/project/devtools/>`_ module used in the
        example below helps format the metadata to be more readable.

        ::

            from devtools import debug

            debug(rm.engines.get_many())

    **List out engines by name**

        This function call lists out the names of your engines, but it can be modified to
        list out other attributes. This is helpful for tracking down a particular engine
        in your account.

        ::

            all_engines = rm.engines.get_many()
            all_engine_names = [e.name for e in all_engines]
            for name in all_engine_names:
                print(name)


    .. note::

        For a list of all engine attributes, see :ref:`model-engine`

Locating an engine
--------------------

Find a specific Firebolt engine by using its name. This function is useful as a
starting point to create an ``engine`` object that can be called in other engine functions.

In the example below, replace the value for ``engine_name`` with your engine name.

    **Locating by name**

        ::

            engine = rm.engines.get("engine_name")


Attaching an engine
---------------------

Attach an engine to a database. An engine must be attached to a database and started before
it can run SQL commands or queries.

    ::

        engine = rm.engines.get("engine_name")
        engine.attach_to_database(
            database=rm.databases.get("database_name")
        )



Dropping an engine
--------------------

Delete an engine by calling the ``delete`` function. The engine is removed from its attached
database and deleted.

    ::

        engine.delete()


Starting an engine
-------------------

Start an engine by calling the ``start`` function on an ``engine`` object. An engine must
be attached to a database and started before it can run SQL commands or queries.

    ::

        engine.start()



Stopping an engine
--------------------

Stop an engine by calling the ``stop`` function. When stopped, an engine is not available
to run queries and does not accrue additional usage time on your account.

    ::

        engine.stop()

Updating an engine
---------------------

Update an engine to change its specifications, returning an updated version of the engine.
The engine must be stopped in order to be updated.

For a list of engine parameters that can be updated, see :meth:`~firebolt.model.engine.Engine.update`

    ::

        engine.update(description = "This is a new description.")

Getting engine status
----------------------

Use the Python `devtools <https://pypi.org/project/devtools/>`_ module to format metadata
from an ``engine`` object. This is a helpful command to run after an engine operation to
check if its execution was successful.

    ::

        from devtools import debug
        debug(engine)

