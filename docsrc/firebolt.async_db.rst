==========================
Async DB
==========================

The Async DB package enables connecting to a Firebolt database for `client-side` asynchronous queries. For running queries in `server-side` asynchronous mode see :ref:`server-side asynchronous query execution`.

:: _async-connect
Connect
------------------------------------

.. automodule:: firebolt.async_db.connect
   :members:
   :undoc-members:
   :show-inheritance:

.. _async-connection:

Connection
------------------------------------

.. note::
   Do not use **connection** directly. Instead, use **connect** as shown above.

.. automodule:: firebolt.async_db.connection
   :members:
   :inherited-members:
   :exclude-members: BaseConnection, async_connect_factory, OverriddenHttpBackend
   :undoc-members:
   :show-inheritance:

.. _async-cursor:

Cursor
--------------------------------

.. automodule:: firebolt.async_db.cursor
   :members:
   :exclude-members: BaseCursor, check_not_closed, check_query_executed, is_db_available, is_engine_running
   :undoc-members:
   :show-inheritance:

Util
------------------------------

.. automodule:: firebolt.async_db.util
   :members:
   :undoc-members:
   :show-inheritance:
