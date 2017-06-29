==================

Defines a %%sparkcache cell magic in the IPython notebook to cache DataFrame
and outputs of long-lasting computations in a persistent ORC file in Hadoop.
Useful when some computations in a notebook are long and you want to
easily save the results in a file.

Installation
------------

-  ``pip install ispark_cache``

Usage
-----

-  In IPython:

   ::

       %load_ext isparkcache

-  Then, create a cell with:

   ::

       %%sparkcache mycache df1 df2
       df1 = sql.createDataFrame(df)
       df2 = sql.createDataFrame(df)

-  When you execute this cell the first time, the code is executed, and
   the variables ``var1`` and ``var2`` are saved in ``mycache.pkl`` in
   the current directory along with the outputs. Rich display outputs
   are only saved if you use the development version of IPython. When
   you execute this cell again, the code is skipped, the variables are
   loaded from the file and injected into the namespace, and the outputs
   are restored in the notebook.

-  Alternatively use ``$file_name`` instead of ``mycache.pkl``, where
   ``file_name`` is a variable holding the path to the file used for
   caching.

-  Use the ``--force`` or ``-f`` option to force the cell's execution
   and overwrite the file.

-  Use the ``--read`` or ``-r`` option to prevent the cell's execution
   and always load the variables from the cache. An exception is raised
   if the file does not exist.

-  Use the ``--cachedir`` or ``-d`` option to specify the cache
   directory. You can specify a default directory in the IPython
   configuration file in your profile (typically in
   ``~\.ipython\profile_default\ipython_config.py``) by adding the
   following line:

   ::

       c.CacheMagics.cachedir = "/path/to/mycache"

   If both a default cache directory and the ``--cachedir`` option are
   given, the latter is used.
