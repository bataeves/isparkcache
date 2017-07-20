Defines a **%%sparkcache** cell magic in the IPython notebook to cache
DataFrame and outputs of long-lasting computations in a persistent
Parquet file in Hadoop. Useful when some computations in a notebook are
long and you want to easily save the results in a file.


Based on `ipycache <https://github.com/rossant/ipycache>`__ module.

Installation
------------

-  ``pip install isparkcache``

Usage
-----

-  In IPython/Jupyter:

    .. code:: python

        %load_ext isparkcache

-  Then, create a cell with:

    .. code:: python

        %%sparkcache df1 df2

        df = ...
        df1 = sql.createDataFrame(df)
        df2 = sql.createDataFrame(df)

-  When you execute this cell the first time, the code is executed, and
   the dataframes ``df1`` and ``df2`` are saved in
   ``/user/$USER/sparkcache/mysparkapplication/df1`` and
   ``/user/$USER/sparkcache/mysparkapplication/df2``. When you execute
   this cell again, the code is skipped, the dataframes are loaded from
   the Parquet and injected into the namespace, and the outputs are
   restored in the notebook.

-  Use the ``--force`` or ``-f`` option to force the cell's execution
   and overwrite the file.

-  Use the ``--read`` or ``-r`` option to prevent the cell's execution
   and always load the variables from the cache. An exception is raised
   if the file does not exist.

-  Use the ``--cachedir`` or ``-d`` option to specify the cache
   directory. Default directory: ``/user/$USER/sparkcache``. You can
   specify a default directory in the IPython configuration file in your
   profile (typically in
   ``~\.ipython\profile_default\ipython_config.py``) by adding the
   following line:

        c.SparkCacheMagics.cachedir = "/path/to/mycache"

If both a default cache directory and the ``--cachedir`` option are
given, the latter is used.
