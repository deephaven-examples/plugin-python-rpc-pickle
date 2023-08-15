from typing import List, Any
from deephaven.plugin import Registration, Callback
from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream

from . import RemoteShell


class RemoteShellConnection(MessageStream):
    def __init__(self, shell: RemoteShell, client_connection: MessageStream):
        self.shell = shell
        self.client_connection = client_connection

    def on_data(self, payload: bytes, references: List[Any]):
        result_payload, result_references = self.shell.execute(payload, references)
        self.client_connection.on_data(result_payload, result_references)

    def on_close(self):
        pass


class RemoteShellObjectType(BidirectionalObjectType):
    @property
    def name(self) -> str:
        return "run_func"

    def is_type(self, object) -> bool:
        return isinstance(object, RemoteShell)

    def create_client_connection(self, obj: RemoteShell, connection: MessageStream) -> MessageStream:
        return RemoteShellConnection(obj, connection)


class RpcPicklePluginRegistration(Registration):
    @classmethod
    def register_into(cls, callback: Callback) -> None:
        callback.register(RemoteShellObjectType)