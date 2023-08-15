# Pickle RPC plugin for Deephaven client

This is an example client-side plugin for the Python client for Deephaven, providing a Python class
to handle calls to be made to and from the server's corresponding plugin.

To install directly to the current venv, invoke `pip install .` from within this directory.

To build the wheel, invoke `python -m build`. Then install the wheel created in `dist/` to
the venv that will be used by the client.

With the server already running with its plugin installed, connect with the pydeehaven client.
Create a plugin client for the server's RemoteShell instance, then invoke commands on it

```python
from pydeephaven import Session
s = Session()
shell_ticket = s.exportable_objects["shell"]
shell_plugin = s.plugin_client(shell_ticket)

from dhexample.rpc_pickle_client import RemoteShellProxy
shell = RemoteShellProxy(shell_plugin)
string_result = shell.run("make_string", 1, 2, 3)
print(string_result)
```