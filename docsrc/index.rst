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

* Python version 3.7 or later along with the pip package installer. For more information, see the `Python <https://www.python.org/downloads/>`_ web page.

* A Firebolt account and login credentials.

========================
Installation
========================

Use pip to install the Firebolt Python SDK from the command line as shown in the example below: 

``$ pip install firebolt-sdk`` 


Optional features 
^^^^^^^^^^^^^^^^^^^

By default, the Firebolt Python SDK uses the ``datetime`` module to parse date and datetime values. For large operations involving date and datetime values, the Python SDK can achieve faster results by using the `ciso8601 <https://pypi.org/project/ciso8601/>`_ package, however this can cause installation issues in some cases.  

To install firebolt-python-sdk with ``ciso8601`` support, run ``pip install firebolt-sdk[ciso8601]``.


Release notes
^^^^^^^^^^^^^^

For information about changes in the latest version of the Firebolt Python SDK, see the `release notes <https://github.com/firebolt-db/firebolt-python-sdk/releases>`_


Contributing
^^^^^^^^^^^^^^

For procedures and requirements for contributing to this SDK, see the `contributing <https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD>`_ page on Github. 

License
^^^^^^^^

The Firebolt DB API is licensed under the `Apache License Version 2.0 <https://github.com/firebolt-db/firebolt-sdk/tree/main/LICENSE>`_ software license.

.. note:: 

   This project is under active development.

========================================



Walkthroughs and examples
===========================

.. toctree::
   :maxdepth: 1

   Connecting and querying <Connecting_and_queries>
   Managing resources <Managing_resources>


Reference documentation
========================

.. toctree::
   :maxdepth: 1

   Async_db <firebolt.async_db>
   Client <firebolt.client>
   Common <firebolt.common>
   Db <firebolt.db>
   Model <firebolt.model>
   Service <firebolt.service>



Indices and tables
==================

* :ref:`genindex`
