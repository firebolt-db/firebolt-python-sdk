==========================
Async DB
==========================

The Async DB package enables asynchronous API calls to a Firebolt database, allowing
client-side processes to continue to run while waiting for API responses. For executing
queries asynchronously `server-side` see :ref:`server-side asynchronous query execution`.

Connect
------------------------------------

.. automodule:: firebolt.async_db.connect
   :members:
   :undoc-members:
   :show-inheritance:

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

Cursor
--------------------------------

.. automodule:: firebolt.async_db.cursor
   :members:
   :exclude-members: BaseCursor, check_not_closed, check_query_executed, is_db_available, is_engine_running
   :undoc-members:
   :show-inheritance:

..

   Util
   ------------------------------

   .. automodule:: firebolt.async_db.util
      :members:
      :undoc-members:
      :show-inheritance:
