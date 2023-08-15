import io
from typing import Any, List
import pydeephaven.experimental.server_object
from pydeephaven.experimental import plugin_client, server_object
import pickle


class _ExportingUnpickler(pickle.Unpickler):
    """
    Given a bytes payload and list of client-side objects that reference server resources,
    enables unpickling those bytes and interpolating the server resources.
    """
    def __init__(self, data, references: List[Any]):
        super().__init__(data)
        self.references = references

    def persistent_load(self, pid):
        return self.references[pid]


class _ExportingPickler(pickle.Pickler):
    """
    Supports pickling data, where any server reference will be appended to an array instead
    of putting its data in the payload.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.references = []

    def persistent_id(self, obj: Any) -> Any:
        if isinstance(obj, pydeephaven.experimental.server_object.ServerObject):
            self.references.append(obj)
            return len(self.references) - 1
        return None


class RemoteShellProxy(server_object.ServerObject):
    """
    Provides access to a RemoteShell instance on the server, enabling a user to call functions
    defined in the server-side scope, passing both picklable python data and ServerObject
    references like tables or other plugins.
    """
    def __init__(self, shell_plugin: plugin_client.PluginClient):
        self.type_ = shell_plugin.type_
        self.ticket = shell_plugin.ticket
        self.shell_plugin = shell_plugin

    def run(self, func_name: str, *args):
        """
        Given a function name to execute on the server and arguments that can be pickled, serializes
        both and sends them to the server, returning the results.
        """
        data = io.BytesIO()
        pickler = _ExportingPickler(data)
        pickler.dump((func_name, *args))

        self.shell_plugin.req_stream.write(data.getvalue(), pickler.references)

        result_bytes, result_references = next(self.shell_plugin.resp_stream)
        buffer = io.BytesIO(result_bytes)
        return _ExportingUnpickler(buffer, result_references).load()
