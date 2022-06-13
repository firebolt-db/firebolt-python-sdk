==========================
Async\_db
==========================

The async_db package enables connecting to a Firebolt database for asynchronous queries.

Async\_db.connect
------------------------------------

.. automodule:: firebolt.async_db.connect
   :members:
   :inherited-members:
   :undoc-members:
   :show-inheritance:

Async\_db.connection
------------------------------------

.. note::
   Do not use **connection** directly. Instead, use **connect** as shown above.

.. automodule:: firebolt.async_db.connection
   :members:
   :inherited-members:
   :exclude-members: BaseConnection, async_connect_factory, OverriddenHttpBackend
   :undoc-members:
   :show-inheritance:

Async\_db.cursor
--------------------------------

.. automodule:: firebolt.async_db.cursor
   :members:
   :exclude-members: BaseCursor, check_not_closed, check_query_executed
   :undoc-members:
   :show-inheritance:

..

   Async\_db.util
   ------------------------------

   .. automodule:: firebolt.async_db.util
      :members:
      :undoc-members:
      :show-inheritance:
