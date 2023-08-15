import io
import pickle
from typing import List, Any

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
    Server-side object wrapping a
    """
    def __init__(self, scope: dict[str, Any]):
        if scope is None:
            self.scope = {}
        else:
            self.scope = scope

    def execute(self, pickled_payload:bytes, references: List[Any]):
        buffer = io.BytesIO(pickled_payload)
        func_name, *args = _ExportingUnpickler(buffer, references).load()

        result = self.scope[func_name](*args)

        data = io.BytesIO()
        pickler = _ExportingPickler(data)
        pickler.dump(result)
        return data.getvalue(), pickler.references
