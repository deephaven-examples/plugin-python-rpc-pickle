# Pickle RPC example for Deephaven

This sample project demonstrates the bidirectional plugin tooling for the python client
and server, and uses Python's `pickle` package to customize how objects are serialized
to enable clients and servers to talk about server objects.

> [!WARNING]
> The `pickle` module is not secure. Please read the advisory at https://docs.python.org/3/library/pickle.html
> before considering using or adapting this example to your own needs.

To use it, install the server plugin in the same venv that your deephaven-core server uses
when running, and install the client plugin in the same venv that your pydeephaven client
is using.