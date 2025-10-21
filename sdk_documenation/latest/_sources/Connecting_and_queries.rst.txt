###############################
Connecting and running queries
###############################

This topic provides a walkthrough and examples for how to use the Firebolt Python SDK to
connect to Firebolt resources to run commands and query data.


Setting up a connection (Cloud)
===============================

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
                    ),
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

Setting up a connection (Core)
===============================

Firebolt Core is a Docker-based version of Firebolt that can be run locally or remotely. To connect to Firebolt Core, you need to use the ``FireboltCore`` authentication class, which doesn't require actual credentials.

To get started, follow the steps below:

**1. Import modules**

    The Firebolt Python SDK requires you to import the following modules before making
    any command or query requests to your Firebolt Core instance.

.. _required_core_connection_imports:

    ::

        from firebolt.db import connect
        from firebolt.client.auth import FireboltCore

**2. Connect to your database**

    To connect to Firebolt Core, you need to create a ``FireboltCore`` auth object and use it
    to establish a connection.

    A connection requires the following parameters:

    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``auth``                           |  Auth object of type FireboltCore. This is a special authentication type that doesn't require credentials.     |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``database``                       |  Optional. The name of the database you would like to connect to. Defaults to "firebolt" if not specified.     |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | ``url``                            |  Optional. The URL of the Firebolt Core instance of form <scheme>://<host>:<port>.                             |
    |                                    |  Defaults to "http://localhost:3473" if not specified.                                                         |
    +------------------------------------+---------------------------------------------------------------------------------------------------------------+

    Here's how to create a connection to Firebolt Core:

    ::

        # Connect to Firebolt Core
        # The database parameter defaults to 'firebolt' if not specified
        with connect(
                auth=FireboltCore(),
                url="http://localhost:3473",
                database="firebolt"
        ) as connection:
            # Create a cursor
            cursor = connection.cursor()

            # Execute a simple test query
            cursor.execute("SELECT 1")

.. note::

    Firebolt Core is assumed to be running locally on the default port (3473). For instructions
    on how to run Firebolt Core locally using Docker, refer to the
    `official docs <https://docs.firebolt.io/firebolt-core/firebolt-core-get-started>`_.


**2.1. Connecting to an HTTPS Server**

    If you are connecting to an HTTPS server running Firebolt Core, ensure the server's certificate is properly configured. Follow these steps:

    - **Obtain the Certificate**: Ensure you have the certificate for the HTTPS server you are connecting to.

    - **Install the Certificate in the System Certificate Store**: For Ubuntu users, copy the certificate to `/usr/local/share/ca-certificates/` and run the following command:

      ```bash
      sudo update-ca-certificates
      ```

      .. note::
         Installing the certificate is also possible on other systems, but you need to use the correct path for your operating system's certificate store.

    - **Provide the Certificate via Environment Variable**: Alternatively, set the `SSL_CERT_FILE` environment variable to the path of your certificate file. For example:

      ```bash
      export SSL_CERT_FILE=/path/to/your/certificate.pem
      ```

    - **Python Version Considerations**: The system certificate store is only available for users running Python 3.10 and above. If you are using an older version of Python, you must explicitly set the `SSL_CERT_FILE` environment variable to use the certificate.


**3. Execute commands using the cursor**

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

There are two supported styles for parameterized queries in the Firebolt Python SDK:

* **QMARK style** (default): Use question marks ``?`` as placeholders. This is controlled by the ``firebolt.db.paramstyle`` variable set to ``"qmark"`` or ``"native"``. Substitution is performed on the client side.
* **FB Numeric style**: Use numbered placeholders ``$1, $2, ...``. This is enabled by setting ``firebolt.db.paramstyle = "fb_numeric"`` before connecting. Substitution is performed on the server side, providing additional protection against SQL injection.

To run a parameterized query, use the ``execute()`` cursor method. Add placeholders to
your statement using the appropriate style, and in the second argument pass a tuple of
parameters equal in length to the number of placeholders in the statement.

**QMARK style example (default):**

::

    # No need to set paramstyle, it defaults to "qmark"

    cursor.execute(
        """
        CREATE FACT TABLE IF NOT EXISTS test_table2 (
            id INT,
            name TEXT,
            date_value DATE
        )
        PRIMARY INDEX id;"""
    )

    cursor.execute(
        "INSERT INTO test_table2 VALUES (?, ?, ?)",
        (1, "hello", "2018-01-01"),
    )


**fb_numeric style example (server-side substitution):**

::

    import firebolt.db
    firebolt.db.paramstyle = "fb_numeric"

    cursor.execute(
        "INSERT INTO test_table2 VALUES ($1, $2, $3)",
        (2, "world", "2018-01-02"),
    )

    # paramstyle only needs to be set once, it will be used for all subsequent queries

    cursor.execute(
        "INSERT INTO test_table2 VALUES ($1, $2, $3)",
        (3, "!", "2018-01-03"),
    )


.. _parameterized_query_executemany_example:

If you need to run the same statement multiple times with different parameter inputs,
you can use the ``executemany()`` cursor method. This allows multiple tuples to be passed
as values in the second argument.

::

    import firebolt.db
    # Explicitly set paramstyle to "qmark" for QMARK style in case it was changed
    firebolt.db.paramstyle = "qmark"

    cursor.executemany(
        "INSERT INTO test_table2 VALUES (?, ?, ?)",
        (
            (2, "banana", "2019-01-01"),
            (3, "carrot", "2020-01-01"),
            (4, "donut", "2021-01-01")
        )
    )

    cursor.close()


Bulk insert for improved performance
--------------------------------------

For inserting large amounts of data more efficiently, you can use the ``bulk_insert`` parameter
with ``executemany()``. This concatenates multiple INSERT statements into a single batch request,
which can significantly improve performance when inserting many rows.

**Note:** The ``bulk_insert`` parameter only works with INSERT statements and supports both
``fb_numeric`` and ``qmark`` parameter styles. Using it with other statement types will
raise an error.

**Example with QMARK parameter style (default):**

::

    # Using the default qmark parameter style
    cursor.executemany(
        "INSERT INTO test_table VALUES (?, ?, ?)",
        (
            (1, "apple", "2019-01-01"),
            (2, "banana", "2020-01-01"),
            (3, "carrot", "2021-01-01"),
            (4, "donut", "2022-01-01"),
            (5, "eggplant", "2023-01-01")
        ),
        bulk_insert=True  # Enable bulk insert for better performance
    )

**Example with FB_NUMERIC parameter style:**

::

    import firebolt.db
    # Set paramstyle to "fb_numeric" for server-side parameter substitution
    firebolt.db.paramstyle = "fb_numeric"

    cursor.executemany(
        "INSERT INTO test_table VALUES ($1, $2, $3)",
        (
            (1, "apple", "2019-01-01"),
            (2, "banana", "2020-01-01"),
            (3, "carrot", "2021-01-01"),
            (4, "donut", "2022-01-01"),
            (5, "eggplant", "2023-01-01")
        ),
        bulk_insert=True  # Enable bulk insert for better performance
    )

When ``bulk_insert=True``, the SDK concatenates all INSERT statements into a single batch
and sends them to the server for optimized batch processing.


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

	Some parameters are not allowed. `account_id`, `output_format`, `database` and `engine` are
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

Asynchronous Python SDK
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


Server-side asynchronous query execution
==========================================
Firebolt supports server-side asynchronous query execution. This feature allows you to run
queries in the background and fetch the results later. This is especially useful for long-running
queries that you don't want to wait for or maintain a persistent connection to the server.

This feature is not to be confused with the Python SDK's asynchronous functionality, which is
described in the :ref:`Asynchronous query execution <connecting_and_queries:Asynchronous query execution>` section,
used to write concurrent code. Server-side asynchronous query execution is a feature of the
Firebolt engine itself.

Submitting an asynchronous query
--------------------------------

Use :py:meth:`firebolt.db.cursor.Cursor.execute_async` method to run query without maintaing a persistent connection.
This method will return immediately, and the query will be executed in the background. Return value
of execute_async is -1, which is the rowcount for queries where it's not applicable.
`cursor.async_query_token` attribute will contain a token that can be used to monitor the query status.

::

    # Synchronous execution
    cursor.execute("CREATE TABLE my_table (id INT, name TEXT, date_value DATE)")

    # Asynchronous execution
    cursor.execute_async("INSERT INTO my_table VALUES (5, 'egg', '2022-01-01')")
    token = cursor.async_query_token

Trying to access `async_query_token` before calling `execute_async` will raise an exception.

.. note::
    Multiple-statement queries are not supported for asynchronous queries. However, you can run each statement
    separately using multiple `execute_async` calls.

.. note::
    Fetching data via SELECT is not supported and will raise an exception. execute_async is best suited for DML queries.

Monitoring the query status
----------------------------

To check the async query status you need to retrieve the token of the query. The token is a unique
identifier for the query and can be used to fetch the query status. You can store this token
outside of the current process and use it later to check the query status. :ref:`Connection <firebolt.db:Connection>` object
has two methods to check the query status: :py:meth:`firebolt.db.connection.Connection.is_async_query_running` and
:py:meth:`firebolt.db.connection.Connection.is_async_query_successful`.`is_async_query_running` will return True
if the query is still running, and False otherwise. `is_async_query_successful` will return True if the query
has finished successfully, None if query is still running and False if the query has failed.

::

    while(connection.is_async_query_running(token)):
        print("Query is still running")
        time.sleep(1)
    print("Query has finished")

    success = connection.is_async_query_successful(token)
    # success is None if the query is still running
    if success is None:
        # we should not reach this point since we've waited for is_async_query_running
        raise Exception("The query is still running, use is_async_query_running to check the status")

    if success:
        print("Query was successful")
    else:
        print("Query failed")

Cancelling a running query
--------------------------

To cancel a running query, use the :py:meth:`firebolt.db.connection.Connection.cancel_async_query` method. This method
will send a cancel request to the server and the query will be stopped.

::

    token = cursor.async_query_token
    connection.cancel_async_query(token)

    # Verify that the query was cancelled
    running = connection.is_async_query_running(token)
    print(running) # False
    successful = connection.is_async_query_successful(token)
    print(successful) # False


Retrieving asynchronous query information
-----------------------------------------

To get additional information about an async query, use the :py:meth:`firebolt.db.connection.Connection.get_async_query_info` method.
This method returns a list of ``AsyncQueryInfo`` objects, each containing detailed information about the query execution.

::

    token = cursor.async_query_token
    query_info_list = connection.get_async_query_info(token)

    for query_info in query_info_list:
        print(f"Query ID: {query_info.query_id}")
        print(f"Status: {query_info.status}")
        print(f"Submitted time: {query_info.submitted_time}")
        print(f"Rows scanned: {query_info.scanned_rows}")
        print(f"Error message: {query_info.error_message}")


Streaming query results
==============================

By default, the driver will fetch all the results at once and store them in memory.
This does not always fit the needs of the application, especially when the result set is large.
In this case, you can use the `execute_stream` cursor method to fetch results in chunks.

.. note::
    The `execute_stream` method is not supported with :ref:`connecting_and_queries:Server-side asynchronous query execution`. It can only be used with regular queries.

.. note::
    If you enable result streaming, the query execution might finish successfully, but the actual error might be returned while iterating the rows.

Synchronous example:
::

    with connection.cursor() as cursor:
        cursor.execute_stream("SELECT * FROM my_huge_table")
        for row in cursor:
            # Process the row
            print(row)

Asynchronous example:
::
    async with async_connection.cursor() as cursor:
        await cursor.execute_stream("SELECT * FROM my_huge_table")
        async for row in cursor:
            # Process the row
            print(row)

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

Execution timeout
==============================

The Firebolt Python SDK allows you to set a timeout for query execution.
In order to do this, you can call the :meth:`Cursor.execute` or :meth:`Cursor.executemany` function with the
``timeout_seconds`` parameter provided. In case the timeout will be reached before the query execution finishes, the
function will raise a ``QueryTimeoutError`` exception.

::

    cursor.execute(
        "SELECT * FROM test_table;",
        timeout_seconds=5
    )

**Warning**: If running multiple queries, and one of queries times out, all the previous queries will not be rolled back and their result will persist. All the remaining queries will be cancelled.
