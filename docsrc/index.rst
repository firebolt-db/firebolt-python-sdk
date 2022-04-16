.. firebolt-sdk documentation master file, created by
   sphinx-quickstart on Wed Nov 17 11:16:16 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

########################
**Firebolt-python-sdk**
########################

The Firebolt Python SDK enables connecting to Firebolt, managing Firebolt resources and executing queries using a library of Python classes and functions. 

========================
Prerequisites
========================

* The Firebolt Python SDK requires Python version 3.7 or later along with the pip package installer. For more information, see the `Python <https://www.python.org/downloads/>`_ web page.

* You need a Firebolt account and login credentials.

========================
Installation
========================

You can use pip to install the Firebolt Python SDK from the command line as shown in the example below: 

``$ pip install firebolt-sdk`` 

==========================
Getting started
==========================

To use the Python SDK, you must provide your account information. These parameters are used to connect to a Firebolt database to execute commands and queries:

+------------------------------------+-------------------------------------------------------------------+
| ``username``                       |  The email address associated with your Firebolt user.            |
+------------------------------------+-------------------------------------------------------------------+
| ``password``                       |  The password used for connecting to Firebolt.                    |
+------------------------------------+-------------------------------------------------------------------+
| ``database``                       |  The name of the database you would like to connect to.           |
+------------------------------------+-------------------------------------------------------------------+
| ``engine_name`` or ``engine_url``  |  The name or URL of the engine to use for SQL queries.            |
+------------------------------------+-------------------------------------------------------------------+


Optional features 
^^^^^^^^^^^^^^^^^^^

By default, the Firebolt Python SDK uses the ``datetime`` module to parse date and datetime values, but this might be slow for large operations. In order to speed up datetime operations, its possible to use `ciso8601 <https://pypi.org/project/ciso8601/>`_ package. 

To install firebolt-python-sdk with ``ciso8601`` support, run ``pip install firebolt-sdk[ciso8601]``.


Release notes
^^^^^^^^^^^^^^

For information about changes in the latest version of the Firebolt Python SDK, see our `release notes <https://github.com/firebolt-db/firebolt-python-sdk/releases>`_


Contributing
^^^^^^^^^^^^^^

To see the procedures and requirements for contributing, see our `contributing <https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD>`_ page on Github. 

License
^^^^^^^^

The Firebolt DB API is licensed under the `Apache License Version 2.0 <https://github.com/firebolt-db/firebolt-sdk/tree/main/LICENSE>`_ software license.

.. note:: 

   This project is under active development.

========================================

.. toctree::
   :maxdepth: 2

   Connecting and running queries <Connecting_and_queries>
   Managing engines and databases <Managing_resources>
   Async_db <firebolt.async_db>
   Client <firebolt.client>
   Common <firebolt.common>
   Db <firebolt.db>
   Model <firebolt.model>
   Service <firebolt.service>



Indices and tables
==================

* :ref:`genindex`
