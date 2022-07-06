==========================
Async DB
==========================

The Async DB package enables connecting to a Firebolt database for asynchronous queries.

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
