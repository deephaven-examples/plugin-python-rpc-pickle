import io
import pickle
from typing import List, Dict, Any, Callable

import deephaven.table


class _ExportingPickler(pickle.Pickler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.references = []

    def persistent_id(self, obj: Any) -> Any:
        if self.is_dh_object(obj):
            self.references.append(obj)
            return len(self.references) - 1
        return None

    def is_dh_object(self, obj: Any):
        # enhancement: delegate this so the user can control their own exportable types?
        return isinstance(obj, deephaven.table.Table)


class _ExportingUnpickler(pickle.Unpickler):
    def __init__(self, data, references):
        super().__init__(data)
        self.references = references

    def persistent_load(self, pid: Any) -> Any:
        return self.references[pid]


class RemoteShell:
    """
    Server-side object wrapping a scope dict. Pickled payloads can be passed to it, consisting of a tuple
    of a string, for the name of the function to invoke from the scope, and any arguments to pass to that
    function.
    """
    def __init__(self, scope: Dict[str, Callable]):
        """Creates a new RemoteShell instance, with the specified scope to read functions from."""
        if scope is None:
            self.scope = {}
        else:
            self.scope = scope

    def execute(self, pickled_payload:bytes, references: List[Any]):
        """Reads the pickled payload and invokes a function in this object's scope. The first string in
        the picked payload is the name of the function to read from the scope, the remainder are arguments
        to pass to the function. The second argument to this function is a list of references of objects
        already on the server that should be used when unpickling arguments, to substitute server-side
        objects for their client-side representations. Returns a tuple consisting of the pickled results,
        and any references to server side objects that shouldn't be pickled.
        """
        buffer = io.BytesIO(pickled_payload)
        func_name, *args = _ExportingUnpickler(buffer, references).load()

        result = self.scope[func_name](*args)

        data = io.BytesIO()
        pickler = _ExportingPickler(data)
        pickler.dump(result)
        return data.getvalue(), pickler.references
