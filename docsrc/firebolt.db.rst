===================
DB
===================

The DB package enables connecting to a Firebolt database for synchronous queries.

Connect
-----------------------------

.. automodule:: firebolt.db.connect
   :members:
   :inherited-members:
   :undoc-members:
   :show-inheritance:

.. _db-connection:

Connection
-----------------------------

.. note::
   Do not use **connection** directly. Instead, use **connect** as shown above.

.. automodule:: firebolt.db.connection
   :members:
   :inherited-members:
   :undoc-members:
   :show-inheritance:

.. _db-cursor:

Cursor
-------------------------

.. automodule:: firebolt.db.cursor
   :members:
   :exclude-members: is_db_available, is_engine_running
   :undoc-members:
   :show-inheritance:
