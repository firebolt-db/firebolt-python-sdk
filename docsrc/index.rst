.. firebolt-sdk documentation master file, created by
   sphinx-quickstart on Wed Nov 17 11:16:16 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=================
**firebolt-sdk**
=================

Welcome to firebolt-sdk's documentation!

########################
**Installation**
########################

*  Requires Python ``>=3.7``
*  ``pip install firebolt-sdk`` 

##########################
**Connection parameters**
##########################

These parameters are used to connect to a Firebolt database:

* **engine_url** - url for a Firebolt engine to make requests to. This can be retrieved from our web interface, or from the `engine <https://github.com/firebolt-db/firebolt-sdk/tree/main/src/firebolt/model/engine.py>`_ attribute endpoint
* **database** - the name of the database to receive queries
* **username** - Firebolt account username
* **password** - Firebolt account password

Optional parameters

* **api_endpoint** - api hostname for logging in. Defaults to ``api.app.firebolt.io``.

###############
**Examples** 
###############

See `PEP-249 <https://www.python.org/dev/peps/pep-0249>`_ for the DB API reference and specifications. An example `jupyter notebook <https://github.com/firebolt-db/firebolt-sdk/tree/main/examples/dbapi.ipynb>`_ is included to illustrate the use of the Firebolt API.


#######################
**Optional features** 
#######################

By default, firebolt-sdk uses ``datetime`` module to parse date and datetime values, which might be slow for a large amount of operations. In order to speed up datetime operations, it's possible to use `ciso8601 <https://pypi.org/project/ciso8601/>`_ package. 

To install firebolt-sdk with ``ciso8601`` support, run ``pip install firebolt-sdk[ciso8601]``

###################
**Contributing**
###################
See: `CONTRIBUTING.MD <https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD>`_

###################
**License**
###################

The Firebolt DB API is licensed under the `Apache License Version 2.0 <https://github.com/firebolt-db/firebolt-sdk/tree/main/LICENSE>`_ software license.

.. note:: 

   This project is under active development

========================================

.. toctree::
   :maxdepth: 1

   firebolt.async_db
   firebolt.client
   firebolt.common
   firebolt.db
   firebolt.model
   firebolt.service



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
