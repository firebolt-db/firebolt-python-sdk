
###############################
Connecting and running queries
###############################

This topic provides a walkthrough and examples for how to use the Firebolt Python SDK to
connect to Firebolt resources to run commands and query data.


Setting up a connection
=========================

To connect to a Firebolt database to run queries or command, you must provide your account
credentials through a connection request.

To get started, follow the steps below:

**1. Import modules**

    The Firebolt Python SDK requires you to import the following modules before making
    any command or query requests to your Firebolt database.

.. _required_connection_imports:

    ::

        from firebolt.db import connect
        from firebolt.client.auth import ClientCredentials


.. _connecting_with_credentials_example:

**2. Connect to your database and engine**


    Your account information can be provided as parameters in a ``connection()`` function.

    A connection requires the following parameters:

    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``auth``                           |  Auth object, containing your credentials. See :ref:`Auth <firebolt.client:auth>` for more details.           |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``account_name``                   |  The name of the account you're using to connect to Firebolt. Must be specified in order to authenticate.     |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``database``                       |  Optional. The name of the database you would like to connect to.                                             |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``engine_name``                    |  Optional. The name of the engine to use for SQL queries.                                                     |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+

    .. note::
        If you specify ``engine_name`` but not the ``database`` Python SDK will automatically resolve the database for you behind the scenes.

        If an ``engine_name`` is not specified the SDK will not be bound to any engine. In this case, if also no ``database`` is specified
        you can still connect, but queries are limited to database and engine management queries e.g. ``CREATE DATABASE``, ``START ENGINE``, etc.
        To interact with tables in a database you have to provide the ``database`` parameter when connecting with no engine.

    This information can be provided in multiple ways.

        * **Set credentials manually**

            You can manually include your account information in a connection object in
            your code for any queries you want to request.

            Replace the values in the example code below with your Firebolt account
            credentials as appropriate.

            ::

                id = "your_service_account_id"
                secret = "your_service_account_secret"
                engine_name = "your_engine"
                database_name = "your_database"
                account_name = "your_account"

                with connect(
                        engine_name=engine_name,
                        database=database_name,
                        account_name=account_name,
                        auth=ClientCredentials(id, secret),
                ) as connection:
                    cursor = connection.cursor()


        * **Use an .env file**

            Consolidating all of your Firebolt credentials into a ``.env`` file can help
            protect sensitive information from exposure. Create an ``.env`` file with the
            following key-value pairs, and replace the values with your information.

            ::

                FIREBOLT_CLIENT_ID="your_service_account_id"
                FIREBOLT_CLIENT_SECRET="your_service_account_secret"
                FIREBOLT_ENGINE="your_engine"
                FIREBOLT_DB="your_database"
                FIREBOLT_ACCOUNT="your_account"

            Be sure to place this ``.env`` file into your root directory.

            Your connection script can load these environmental variables from the ``.env``
            file by using the `python-dotenv <https://pypi.org/project/python-dotenv/>`_
            package. Note that the example below imports the ``os`` and ``dotenv`` modules
            in order to load the environmental variables.

            ::

                import os
                from dotenv import load_dotenv

                load_dotenv()

                with connect(
                    auth=ClientCredentials(
                        os.getenv("FIREBOLT_CLIENT_ID"),
                        os.getenv("FIREBOLT_CLIENT_SECRET")
                    )
                    engine_name=os.getenv('FIREBOLT_ENGINE'),
                    database=os.getenv('FIREBOLT_DB'),
                    account_name=os.getenv('FIREBOLT_ACCOUNT'),
                ) as connection:
                    cursor = connection.cursor()


**3. Token management/caching**

	Firebolt allows access by using authentication and refresh tokens.  In order to authenticate,
	the SDK issues an http login request to the Firebolt API, providing username and password.
	The API returns an authentication token and refresh token.   Authentication tokens are valid
	for 12 hours, and can be refreshed using the refresh token.  The SDK uses the authentication
	token for all subsequent requests, and includes logic for refreshing the token if it is reported as expired.

	Because a typical script that uses the SDK usually runs for less than a minute and then is closed,
	the token is lost because it’s only stored in a process memory.  To avoid that, the SDK by default does token caching.
	Token caching is designed to preserve the token in filesystem to later reuse it for requests and save time on
	authentication api request. It also helps for workflows that use the SDL in parallel or in sequential scripts
	on the same machine, as only a single authentication request is performed.  The caching works by preserving the
	token value and it’s expiration timestamp in filesystem, in user data directory. On the authentication, the SDK
	first tries to find a token cache file and, if it exists, checks that token is not yet expired. If the token
	is valid, it’s used for further authorization. The token value itself is encrypted with PBKDF2 algorithm,
	the encryption key is a combination of user credentials.

	Token caching can be disabled if desired.  If the server the SDK is running on has a read only
	filesystem (when using AWS Lambda, for example), then the SDK will not be able to store the token.
	The caching is disabled by adding ``use_token_cache=False`` to the auth object.  From the examples above,
	it would look like: ``auth=UsernamePassword(username, password,use_token_cache=False),``


**4. Execute commands using the cursor**

    The ``cursor`` object can be used to send queries and commands to your Firebolt
    database and engine. See below for examples of functions using the ``cursor`` object.

Synchronous command and query examples
==================================================

This section includes Python examples of various SQL commands and queries.


Inserting and selecting data
-----------------------------

.. _basic_execute_example:

The example below uses ``cursor`` to create a new table called ``test_table``, insert
rows into it, and then select the table's contents.

The engine attached to your specified database must be started before executing any
queries. For help, see :ref:`managing_resources:starting an engine`.

::

    cursor.execute(
        """
        CREATE FACT TABLE IF NOT EXISTS test_table (
            id INT,
            name TEXT
        )
        PRIMARY INDEX id;
        """
    )

    cursor.execute(
        """
        INSERT INTO test_table VALUES
        (1, 'hello'),
        (2, 'world'),
        (3, '!');
        """
    )

    cursor.execute("SELECT * FROM test_table;")

    cursor.close()

.. note::

    For reference documentation on ``cursor`` functions, see :ref:`cursor <firebolt.db:cursor>`.


Fetching query results
-----------------------

After running a query, you can fetch the results using a ``cursor`` object. The examples
below use the data queried from ``test_table`` created in the
:ref:`connecting_and_queries:Inserting and selecting data`.

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

	::

		print(cursor.fetchall())

	**Returns**: ``[[2, 'world'], [1, 'hello'], [3, '!']]``

Fetching query result information
-----------------------

After running a query, you can fetch information about the results using the same ``cursor`` object. The examples
below are from the last SELECT query in :ref:`connecting_and_queries:Inserting and selecting data`.

.. _result_information_example:

**rowcount**

	- For a SELECT query, rowcount is the number of rows selected.
	- For An INSERT query, it is always -1.
	- For DDL (CREATE/DROP), it is always 1

	::

		print("Rowcount: ", cursor.rowcount)

	**Returns**: ``Rowcount:  3``


**description**

	description is a list of Column objects, each one responsible for a single column in a result set. Only name and type_code fields get populated, all others are always empty.

	- name is the name of the column.
	- type_code is the data type of the column.  It can be:

		- a python type (int, float, str, date, datetime)
		- an ARRAY object, that signifies a list of some type. The inner type can is stored in ``.subtype`` field
		- a DECIMAL object, that signifies a decimal value. It’s precision and scale are stored in ``.precision`` and ``.scale`` fields
		- a DATETIME64 object, that signifies a datetime value with an extended precision. The precision is stored in ``.precision``

	::

		print("Description: ", cursor.description)

	**Returns**: ``Description:  [Column(name='id', type_code=<class 'int'>, display_size=None, internal_size=None, precision=None, scale=None, null_ok=None), Column(name='name', type_code=<class 'str'>, display_size=None, internal_size=None, precision=None, scale=None, null_ok=None)]``



Executing parameterized queries
---------------------------------

.. _parameterized_query_execute_example:

Parameterized queries (also known as “prepared statements”) format a SQL query with
placeholders and then pass values into those placeholders when the query is run. This
protects against SQL injection attacks and also helps manage dynamic queries that are
likely to change, such as filter UIs or access control.

To run a parameterized query, use the ``execute()`` cursor method. Add placeholders to
your statement using question marks ``?``, and in the second argument pass a tuple of
parameters equal in length to the  number of ``?`` in the statement.


::

    cursor.execute(
        """
        CREATE FACT TABLE IF NOT EXISTS test_table2 (
            id INT,
            name TEXT,
            date_value DATE
        )
        PRIMARY INDEX id;"""
    )


::

    cursor.execute(
        "INSERT INTO test_table2 VALUES (?, ?, ?)",
        (1, "apple", "2018-01-01"),
    )

    cursor.close()

.. _parameterized_query_executemany_example:

If you need to run the same statement multiple times with different parameter inputs,
you can use the ``executemany()`` cursor method. This allows multiple tuples to be passed
as values in the second argument.

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


Setting session parameters
--------------------------------------

Session parameters are special SQL statements allowing you to modify the behavior of
the current session. For example, you can set the time zone for the current session
using the ``SET time_zone`` statement. More information on session parameters can be
found in the relevant `section <https://docs.firebolt.io/godocs/Reference/system-settings.html>`_
in Firebolt docs.

In Python SDK session parameters are stored on the cursor object and are set using the
``execute()`` method. This means that each cursor you create will act independently of
each other. Any session parameters on one will have no effect on another cursor.
The example below sets the time zone to UTC and then selects a timestamp with time zone.

::

	cursor.execute("SET time_zone = 'UTC'")
	cursor.execute("SELECT TIMESTAMPTZ '1996-09-03 11:19:33.123456 Europe/Berlin'")

Alternatively set paramters can be set in a multi-statement query.

::

	cursor.execute("SET time_zone = 'UTC'; SELECT TIMESTAMPTZ '1996-09-03 11:19:33.123456 Europe/Berlin'")

Even when set in a multi-statement query, the session parameters will be set for the
entire session, not just for the duration of the query. To reset the parameter either
set it to a new value or use `flush_parameters()` method.

::

	cursor.flush_parameters()


.. note::

	Some parameters are not allowed. `account_id`, `output_format`, `database`and `engine` are
	internal parameters and should not be set using the `SET` statement. Database and engine
	parameters (if enabled on your Firebolt version) can be set via `USE DATABASE` and `USE ENGINE`.


Executing multiple-statement queries
--------------------------------------

Multiple-statement queries allow you to run a series of SQL statements sequentially with
just one method call. Statements are separated using a semicolon ``;``, similar to making
SQL statements in the Firebolt UI.

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

    First query: [[2, 'banana', datetime.date(2019, 1, 1)],
                  [3, 'carrot', datetime.date(2020, 1, 1)],
                  [1, 'apple', datetime.date(2018, 1, 1)]]
    Second query: [[3, 'carrot', datetime.date(2020, 1, 1)],
                   [4, 'donut', datetime.date(2021, 1, 1)]]

.. note::

    Multiple statement queries are not able to use placeholder values for parameterized queries.



Asynchronous query execution
==========================================

Not to be confused with :ref:`Server-side async`. Asynchronous Python SDK
functionality is used to write concurrent code. Unlike in a synchronous approach, when executing
a query is a blocking operation, this approach allows doing other processing or queries while the
original query is waiting on the network or the server to respond. This is especially useful when
executing slower queries.

Make sure you're familiar with the `Asyncio approach <https://docs.python.org/3/library/asyncio.html>`_
before using asynchronous Python SDK, as it requires special async/await syntax.


Simple asynchronous example
---------------------------

This example illustrates a simple query execution via the async Python SDK. It does not have any
performance benefits, but rather shows the difference in syntax from the synchronous version.
It can be extended to run alongside of other operations.

::

    from asyncio import run
    from firebolt.async_db import connect as async_connect
    from firebolt.client.auth import ClientCredentials


    async def run_query():
        id = "your_service_account_id"
        secret = "your_service_account_secret"
        engine_name = "your_engine"
        database_name = "your_database"
        account_name = "your_account"

        query = "select * from my_table"

        async with await async_connect(
            engine_name=engine_name,
            database=database_name,
            account_name=account_name,
            auth=ClientCredentials(id, secret),
        ) as connection:
            cursor = connection.cursor()

            # Asyncronously execute a query
            rowcount = await cursor.execute(query)

            # Asyncronously fetch a result
            single_row = await cursor.fetchone()
            multiple_rows = await cursor.fetchmany(5)
            all_remaining_rows = await cursor.fetchall()

    # Run async `run_query` from the synchronous context of your script
    run(run_query())


Running multiple queries in parallel
------------------------------------

Building up on the previous example, we can execute several queries concurently.
This is especially useful when queries do not depend on each other and can be run
at the same time.

::

    from asyncio import gather, run
    from firebolt.async_db import connect as async_connect
    from firebolt.client.auth import ClientCredentials


    async def execute_sql(connection, query):
        # Create a new cursor for every query
        cursor = connection.cursor()
        # Wait for cursor to execute a query
        await cursor.execute(query)
        # Return full query result
        return await cursor.fetchall()


    async def run_multiple_queries():
        id = "your_service_account_id"
        secret = "your_service_account_secret"
        engine_name = "your_engine"
        database_name = "your_database"
        account_name = "your_account"

        queries = [
            "select * from table_1",
            "select * from table_2",
            "select * from table_3",
        ]

        async with await async_connect(
            engine_name=engine_name,
            database=database_name,
            account_name=account_name,
            auth=ClientCredentials(id, secret),
        ) as connection:
            # Create async tasks for every query
            tasks = [execute_sql(connection, query) for query in queries]
            # Execute tasks concurently
            results = await gather(*tasks)
            # Print query results
            for i, result in enumerate(results):
                print(f"Query {i}: {result}")


    run(run_multiple_queries())

.. note::
    This will run all queries specified in ``queries`` list at the same time. With heavy queries you
    have to be mindful of the engine capability here. Excessive parallelisations can lead to degraded
    performance. You should also make sure the machine running this code has enough RAM to store all
    the results you're fetching.

    :ref:`concurrent limit` suggests a way to avoid this.


.. _Concurrent limit:

Limiting number of conccurent queries
-------------------------------------

It's generally a good practice to limit a number of queries running at the same time. It ensures a
load on both server and client machines can be controlled. A suggested way is to use the
`Semaphore <https://docs.python.org/3/library/asyncio-sync.html#semaphore>`_.

::

    from asyncio import gather, run, Semaphore
    from firebolt.async_db import connect as async_connect
    from firebolt.client.auth import ClientCredentials


    MAX_PARALLEL = 2


    async def gather_limited(tasks, max_parallel):
        sem = Semaphore(max_parallel)

        async def limited_task(task):
            async with sem:
                await task

        await gather(*[limited_task(t) for t in tasks])


    async def execute_sql(connection, query):
        # Create a new cursor for every query
        cursor = connection.cursor()
        # Wait for cursor to execute a query
        await cursor.execute(query)
        # Return full query result
        return await cursor.fetchall()


    async def run_multiple_queries():
        id = "your_service_account_id"
        secret = "your_service_account_secret"
        engine_name = "your_engine"
        database_name = "your_database"
        account_name = "your_account"

        queries = [
            "select * from table_1",
            "select * from table_2",
            "select * from table_3",
        ]

        async with await async_connect(
            engine_name=engine_name,
            database=database_name,
            account_name=account_name,
            auth=ClientCredentials(id, secret),
        ) as connection:
            # Create async tasks for every query
            tasks = [execute_sql(connection, query) for query in queries]
            # Execute tasks concurently, limiting the parallelism
            results = await gather_limited(*tasks, MAX_PARALLEL)
            # Print query results
            for i, result in enumerate(results):
                print(f"Query {i}: {result}")


    run(run_multiple_queries())

.. _Server-side async:

Server-side asynchronous query execution
==========================================

In addition to :ref:`asynchronous API calls <firebolt.async_db:async db>`, which allow `client-side`
execution to continue while waiting for API responses, the Python SDK provides `server-side`
asynchronous query execution. When a query is executed asynchronously the only response
from the server is a query ID. The status of the query can then be retrieved by polling
the server at a later point. This frees the connection to do other queries or even be
closed while the query continues to run. And entire service, such as AWS Lamdba, could
potentially even be spun down an entire while a long-running database job is still underway.

Note, however, that it is not possible to retrieve the results of a server-side asynchronous
query, so these queries are best used for running DMLs and DDLs and ``SELECT``\ s should be used
only for warming the cache.

Executing asynchronous DDL commands
------------------------------------

.. _ddl_execution_example:

Executing queries server-side asynchronously is similar to executing server-side synchronous
queries, but the ``execute()`` command receives an extra parameter, ``async_execution=True``.
The example below uses ``cursor`` to create a new table called ``test_table``.
``execute(query, async_execution=True)`` will return a query ID, which can subsequently
be used to check the query status.

::

    query_id = cursor.execute(
        """
        CREATE FACT TABLE IF NOT EXISTS test_table (
            id INT,
            name TEXT
        )
        PRIMARY INDEX id;
        """,
        async_execution=True
    )


To check the status of a query, send the query ID to ```get_status()``` to receive a
QueryStatus enumeration object. Possible statuses are:


    * ``RUNNING``
    * ``ENDED_SUCCESSFULLY``
    * ``ENDED_UNSUCCESSFULLY``
    * ``NOT_READY``
    * ``STARTED_EXECUTION``
    * ``PARSE_ERROR``
    * ``CANCELED_EXECUTION``
    * ``EXECUTION_ERROR``


Once the status of the table creation is ``ENDED_SUCCESSFULLY``, data can be inserted into it:

::

    from firebolt.async_db.cursor import QueryStatus

    query_status = cursor.get_status(query_id)

    if query_status == QueryStatus.ENDED_SUCCESSFULLY:
        cursor.execute(
            """
            INSERT INTO test_table VALUES
                (1, 'hello'),
                (2, 'world'),
                (3, '!');
            """
        )


In addition, server-side asynchronous queries can be cancelled calling ``cancel()``.

::

    query_id = cursor.execute(
        """
        CREATE FACT TABLE IF NOT EXISTS test_table (
            id INT,
            name TEXT
        )
        PRIMARY INDEX id;
        """,
        async_execution=True
    )

    cursor.cancel(query_id)

    query_status = cursor.get_status(query_id)

    print(query_status)

**Returns**: ``CANCELED_EXECUTION``


Thread safety
==============================

Thread safety is set to 2, meaning it's safe to share the module and
:ref:`Connection <firebolt.db:Connection>` object across threads.
:ref:`Cursor <firebolt.db:Cursor>` is a lightweight object that should be instantiated
by calling ``connection.cursor()`` within a thread and should not be shared across different threads.
Similarly, in an asynchronous context the Cursor obejct should not be shared across tasks
as it will lead to a nondeterministic data returned. Follow the best practice from the
:ref:`connecting_and_queries:Running multiple queries in parallel`.


Using DATE and DATETIME values
==============================

DATE, DATETIME and TIMESTAMP values used in SQL insertion statements must be provided in
a specific format; otherwise they could be read incorrectly.

* DATE values should be formatted as **YYYY-MM-DD**

* DATETIME and TIMESTAMP values should be formatted as **YYYY-MM-DD HH:MM:SS.SSSSSS**

The `datetime <https://docs.python.org/3/library/datetime.html>`_ module from the Python
standard library contains various classes and methods to format DATE, TIMESTAMP and
DATETIME data types.

You can import this module as follows:

::

    from datetime import datetime

