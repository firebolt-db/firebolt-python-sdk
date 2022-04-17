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


Optional features 
^^^^^^^^^^^^^^^^^^^

By default, the Firebolt Python SDK uses the ``datetime`` module to parse date and datetime values, but this might be slow for large operations. In order to speed up datetime operations, its possible to use `ciso8601 <https://pypi.org/project/ciso8601/>`_ package. 

To install firebolt-python-sdk with ``ciso8601`` support, run ``pip install firebolt-sdk[ciso8601]``.


Release notes
^^^^^^^^^^^^^^

For information about changes in the latest version of the Firebolt Python SDK, see our `release notes <https://github.com/firebolt-db/firebolt-python-sdk/releases>`_


Contributing
^^^^^^^^^^^^^^

For procedures and requirements for contributing to this SDK, see our `contributing <https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD>`_ page on Github. 

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
