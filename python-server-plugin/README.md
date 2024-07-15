# Pickle RPC plugin for Deephaven server

This is an example server-side plugin for Deephaven, providing both a Python class to invoke commands, and an ObjectType plugin to enable clients to call it with pickled arguments.

To install directly to the current venv, invoke `pip install .` from within this directory.

To build the wheel, invoke `python -m build` (you may need to run `pip install build` if the build module is not
already installed in your environment). Then install the wheel created in `dist/` to the venv that will be used
by the server.

Once the server is running, the server should have an instance of `dhexample.rpc_pickle_server.RemoteShell` created
and bound in the global scope so that clients can reference it by name. Construct the instance
with a `scope=` assigned to a Python dictionary that has function definitions in it to allow
access to from the server.

```python
from dhexample.rpc_pickle_server import RemoteShell
shell = RemoteShell(scope=globals())

def make_string(a, b, c):
    return f"{a} - {b} - {c} #"
```

