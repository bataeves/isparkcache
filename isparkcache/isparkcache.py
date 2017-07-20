# -*- coding: utf-8 -*-
__author__ = "e.bataev@corp.mail.ru"
# -*- coding: utf-8 -*-
"""Defines a %%sparkcache cell magic in the notebook to persistent-cache results of 
long-lasting computations.
"""

# ------------------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------------------

# Stdlib
import re
import sys
import warnings
from os.path import join

from .fs.hdfs import HDFSClient
from .fs.local import LocalFSClient

try:
    fs = HDFSClient()
except Exception:
    warnings.warn("No HDFS Configured. Fall back to LocalFS")
    fs = LocalFSClient()

# Our own
from IPython.core import magic_arguments
from IPython.core.magic import Magics, magics_class, cell_magic
from IPython.display import clear_output
from IPython.utils.io import CapturedIO
from traitlets.config import Configurable
from traitlets import Unicode

# ------------------------------------------------------------------------------
# Six utility functions for Python 2/3 compatibility
# ------------------------------------------------------------------------------
# Author: "Benjamin Peterson <benjamin@python.org>"

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    import pickle, builtins
    from io import StringIO

    _iteritems = "items"

    exec_ = getattr(builtins, "exec")
else:
    import cPickle as pickle
    from StringIO import StringIO

    _iteritems = "iteritems"


    def exec_(_code_, _globs_=None, _locs_=None):
        """Execute code in a namespace."""
        if _globs_ is None:
            frame = sys._getframe(1)
            _globs_ = frame.f_globals
            if _locs_ is None:
                _locs_ = frame.f_locals
            del frame
        elif _locs_ is None:
            _locs_ = _globs_
        exec ("""exec _code_ in _globs_, _locs_""")


def iteritems(d, **kw):
    """Return an iterator over the (key, value) pairs of a dictionary."""
    return iter(getattr(d, _iteritems)(**kw))


# ------------------------------------------------------------------------------
# cloudpickle
# ------------------------------------------------------------------------------

try:
    import cloudpickle

    dump = cloudpickle.dump
except ImportError:
    dump = pickle.dump


# ------------------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------------------
def conditional_eval(var, variables):
    """
    Evaluates the variable string if it starts with $.
    If the variable string contains one or several {code} statements, the code
    is executed and the result stringified (wrapped in str()) into the rest of
    the string.
    """
    if var[0] == '$':
        return variables.get(var[1:], var)

    def evalfun(x):
        code = x.group(0)[1:-1]
        return str(eval(code, variables))

    return re.sub(r'{.*?}', evalfun, var, flags=re.DOTALL)


def clean_var(var):
    """Clean variable name, removing accidental commas, etc."""
    return var.strip().replace(',', '')


def clean_vars(vars):
    """Clean variable names, removing accidental commas, etc."""
    return sorted(map(clean_var, vars))


def do_save(path, force=False, read=False):
    """Return True or False whether the variables need to be saved or not."""
    if force and read:
        raise ValueError(("The 'force' and 'read' options are "
                          "mutually exclusive."))

    # Execute the cell and save the variables.
    return force or (not read and not fs.exists(path))


def load_vars(sql, path, vars):
    """Load variables from a parquet file.

    Arguments:

      * path: the path to the parquet file.
      * vars: a list of variable names.

    Returns:

      * cache: a dictionary {var_name: var_value}.

    """
    cache_vars = {}
    for var in vars:
        try:
            cache_vars[var] = sql.read.parquet(join(path, var))
        except Exception as e:
            raise ValueError("The following variables could not be loaded "
                             "from the cache: {0:s}".format(var))
    return cache_vars


def save_vars(path, vars_d):
    """Save variables into a parquet file.

    Arguments:

      * path: the path to the parquet file.
      * vars_d: a dictionary {var_name: var_value}.

    """
    for var_name, var_value in vars_d.iteritems():
        var_value.write.parquet(join(path, var_name), mode="overwrite")


# ------------------------------------------------------------------------------
# CapturedIO
# ------------------------------------------------------------------------------
def save_captured_io(io):
    return dict(
        stdout=StringIO(io._stdout.getvalue()),
        stderr=StringIO(io._stderr.getvalue()),
        outputs=getattr(io, '_outputs', []),  # Only IPython master has this
    )


def load_captured_io(captured_io):
    try:
        return CapturedIO(captured_io.get('stdout', None),
                          captured_io.get('stderr', None),
                          outputs=captured_io.get('outputs', []),
                          )
    except TypeError:
        return CapturedIO(captured_io.get('stdout', None),
                          captured_io.get('stderr', None),
                          )


class myStringIO(StringIO):
    """class to simultaneously capture and output"""

    def __init__(self, out=None, buf=""):
        self._out = out
        StringIO.__init__(self, buf)

    def write(self, s):
        self._out.write(s)
        StringIO.write(self, s)


import IPython.utils.io


class capture_output_and_print(object):
    """
    Taken from IPython.utils.io and modified to use myStringIO.
    context manager for capturing stdout/err
    """
    stdout = True
    stderr = True
    display = True

    def __init__(self, stdout=True, stderr=True, display=True):
        self.stdout = stdout
        self.stderr = stderr
        self.display = display
        self.shell = None

    def __enter__(self):
        from IPython.core.getipython import get_ipython
        from IPython.core.displaypub import CapturingDisplayPublisher

        self.sys_stdout = sys.stdout
        self.sys_stderr = sys.stderr

        if self.display:
            self.shell = get_ipython()
            if self.shell is None:
                self.save_display_pub = None
                self.display = False

        stdout = stderr = outputs = None
        if self.stdout:
            # stdout = sys.stdout = StringIO()
            stdout = sys.stdout = myStringIO(out=IPython.utils.io.stdout)
        if self.stderr:
            # stderr = sys.stderr = StringIO()
            stderr = sys.stderr = myStringIO(out=self.sys_stderr)
        if self.display:
            self.save_display_pub = self.shell.display_pub
            self.shell.display_pub = CapturingDisplayPublisher()
            outputs = self.shell.display_pub.outputs

        return CapturedIO(stdout, stderr, outputs)

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.sys_stdout
        sys.stderr = self.sys_stderr
        if self.display and self.shell:
            self.shell.display_pub = self.save_display_pub


# -----------------------------------------------------------------------------
# %%cache Magics
# ------------------------------------------------------------------------------
def cache(cell, path, sql, vars=[],
          # HACK: this function implementing the magic's logic is testable
          # without IPython, by giving mock functions here instead of IPython
          # methods.
          ip_user_ns={}, ip_run_cell=None, ip_push=None, ip_clear_output=lambda: None,
          force=False, read=False, verbose=True):
    if not path:
        raise ValueError("The path needs to be specified as a first argument.")

    io = None

    need_run = False
    for var in vars:
        if do_save(join(path, var), force=force, read=read):
            need_run = True

    if need_run:
        # Capture the outputs of the cell.
        with capture_output_and_print() as io:
            try:
                ip_run_cell(cell)
            except:
                # Display input/output.
                io()
                return
        # Create the cache from the namespace.
        try:
            cached = {var: ip_user_ns[var] for var in vars}
        except KeyError:
            vars_missing = set(vars) - set(ip_user_ns.keys())
            vars_missing_str = ', '.join(["'{0:s}'".format(_)
                                          for _ in vars_missing])
            raise ValueError(("Variable(s) {0:s} could not be found in the "
                              "interactive namespace").format(vars_missing_str))
        # Save the cache in the pickle file.
        save_vars(path, cached)
        ip_clear_output()  # clear away the temporary output and replace with the saved output (ideal?)
        if verbose:
            print("[Saved variables '{0:s}' to directory '{1:s}'.]".format(', '.join(vars), path))

    # If the cache file exists, and no --force mode, load the requested
    # variables from the specified file into the interactive namespace.
    else:
        # Load the variables from cache in inject them in the namespace.
        force_recalc = False
        try:
            cached = load_vars(sql, path, vars)
        except ValueError as e:
            if 'The following variables' in str(e):
                if read:
                    raise
                force_recalc = True
            else:
                raise
            cached = {}
        if force_recalc and not read:
            return cache(cell, path, sql, vars, ip_user_ns, ip_run_cell, ip_push,
                         ip_clear_output, True, read, verbose)
        # Push the remaining variables in the namespace.
        ip_push(cached)
        if verbose:
            print(("[Skipped the cell's code and loaded variables {0:s} "
                   "from directory '{1:s}'.]").format(', '.join(vars), path))

    # Display the outputs, whether they come from the cell's execution
    # or the pickle file.
    if io:
        io()  # output is only printed when loading file


@magics_class
class SparkCacheMagics(Magics, Configurable):
    """Variable caching.

    Provides the %cache magic."""

    cachedir = Unicode('', config=True)

    def __init__(self, shell=None):
        Magics.__init__(self, shell)
        Configurable.__init__(self, config=shell.config)

    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        'vars', nargs='*', type=str,
        help="Variables to save."
    )
    @magic_arguments.argument(
        '-s', '--silent', action='store_true', default=False,
        help="Do not display information when loading/saving variables."
    )
    @magic_arguments.argument(
        '-d', '--cachedir',
        help="Cache directory as an absolute or relative path."
    )
    @magic_arguments.argument(
        '-f', '--force', action='store_true', default=False,
        help="Force the cell's execution and save the variables."
    )
    @magic_arguments.argument(
        '-r', '--read', action='store_true', default=False,
        help=("Always read from the file and prevent the cell's execution, "
              "raising an error if the file does not exist.")
    )
    @cell_magic
    def sparkcache(self, line, cell):
        """Cache user variables in a file, and skip the cell if the cached
        variables exist.

        Usage:

            %%sparkcache df1 df2
            # If /user/$USER/sparkcache/<AppName>/df1 or /user/$USER/sparkcache/<AppName>/df2 doesn't exist, this cell is executed and
            # df1 and df2 are saved in this directories as Parquet.
            # Otherwise, the cell is skipped and these variables are
            # injected from the file to the interactive namespace.
            df1 = ...
            df2 = ...

        """
        ip = self.shell
        args = magic_arguments.parse_argstring(self.sparkcache, line)
        vars = clean_vars(args.vars)

        # The cachedir can be specified with --cachedir or inferred from the
        # path or in ipython_config.py

        from pyspark import SparkContext
        from pyspark.sql import SQLContext

        sc = SparkContext.getOrCreate()
        sql = SQLContext.getOrCreate(sc)
        sc_name = dict(sc.getConf().getAll()).get("spark.app.name")

        cachedir = args.cachedir or self.cachedir or join(fs.homedir(), "sparkcache/", sc_name)

        cache(cell, cachedir, sql=sql, vars=vars,
              force=args.force, verbose=not args.silent, read=args.read,
              # IPython methods
              ip_user_ns=ip.user_ns,
              ip_run_cell=ip.run_cell,
              ip_push=ip.push,
              ip_clear_output=clear_output
              )


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(SparkCacheMagics)
