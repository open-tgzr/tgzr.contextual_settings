from __future__ import annotations
from typing import Any

import logging
import asyncio
import json
import time
import os

import nats
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy
import nats.js.errors
import nats.aio.client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from .base_store import (
    BaseStore,
    ContextData,
    ModelType,
    # ops,
    # expand_context_name,
    expand_context_names,
    # get_environ,
)
from .memory_store import MemoryStore, ModelType
from ..context_data import ContextData

logger = logging.getLogger(__name__)


class RemoteMemoryStore(MemoryStore):

    def _resolve_context_data(
        self, contexts: list[str], with_history: bool = False
    ) -> dict[str, Any]:
        """
        The MemoryStore returns a ContextData here, but it's not serializable
        so we return a dict instead and the client rebuilds the ContextData
        in its _resolve_context_data() method.
        """
        context_data = super()._resolve_context_data(contexts, with_history)
        dict_values = context_data.to_dict()
        # print("----> RemoteMemoryStore._resolve_context_data(...):")
        # print("   ContextData:", context_data)
        # print("   dict_values:", dict_values)
        return dict_values

    if 0:
        # These are not used remotely so we don't need to addapt their signature
        # but someday we might need it so I'll keep it here:
        def _build_context_dict(
            self,
            dict_values: dict[str, Any],
            path: str | None = None,
            with_history: bool = False,
        ) -> dict[str, Any]:
            values = ContextData(**dict_values)
            return super()._build_context_dict(values, path, with_history)

        if 0:
            # This one is never called from the client, it cannot send
            # the resulting ModelType...
            # (client is building it itself)
            def _build_context(
                self,
                dict_values: dict[str, Any],
                model_type: type[ModelType],
                path: str | None = None,
            ) -> ModelType:
                values = ContextData(**dict_values)
                return super()._build_context(values, model_type, path)


class JetStreamStoreService:

    def __init__(
        self, nc: nats.aio.client.Client, stream_name: str, subject_prefix: str
    ):
        super().__init__()
        self._backend_store = RemoteMemoryStore()

        self._nc = nc
        self._js = self._nc.jetstream()

        self._stream_name = stream_name
        self._subject_prefix = subject_prefix

        self._cmd_subject = f"{subject_prefix}.$CMD.>"
        self._cmd_sub: JetStreamContext.PushSubscription | None = None
        self._query_subject = f"{subject_prefix}.$QUERY.>"
        self._query_sub: Subscription | None = None

        self._alive = False

    async def connect(self) -> bool:
        try:
            self._cmd_sub = await self._js.subscribe(
                self._cmd_subject,
                ordered_consumer=True,
                deliver_policy=DeliverPolicy.ALL,
                cb=self._on_cmd_msg,
            )
        except nats.js.errors.NotFoundError:
            print(
                f"Could not subscribe to {self._cmd_subject!r} (stream: {self._stream_name!r})"
            )
            return False
        else:
            print("Subcribed to", self._cmd_sub.subject)

        try:
            self._query_sub = await self._nc.subscribe(
                self._query_subject,
                cb=self._on_query_msg,
            )
        except nats.js.errors.NotFoundError:
            print(f"Could not subscribe to {self._query_subject!r}")
            return False
        else:
            print("Subcribed to", self._query_sub.subject)

        return True

    async def disconnet(self):
        if self._cmd_sub is not None:
            # await self._cmd_sub.drain()
            await self._cmd_sub.unsubscribe()

        if self._query_sub is not None:
            await self._query_sub.unsubscribe()

    async def _on_cmd_msg(self, msg: Msg):
        cmd = msg.subject.split("$CMD.")[-1]
        data = msg.data.decode()
        try:
            kwargs = json.loads(data)
        except json.decoder.JSONDecodeError as err:
            print("Bad cmd payload:", err)
            return
        self.execute_cmd(cmd, kwargs)
        await msg.ack()

    async def _on_query_msg(self, msg: Msg):
        cmd = msg.subject.split("$QUERY.")[-1]
        data = msg.data.decode()
        kwargs = json.loads(data)
        result = self.execute_query(cmd, kwargs)
        payload = json.dumps(result)
        await self._nc.publish(msg.reply, payload.encode())

    def execute_cmd(self, cmd_name, kwargs):
        print("CMD:", cmd_name, kwargs)
        try:
            meth = getattr(self._backend_store, cmd_name)
        except AttributeError:
            print(f"    > {cmd_name} ERROR: unknown cmd")
            return
        try:
            meth(**kwargs)
        except Exception as err:
            print(f"    > {cmd_name} ERROR:", err)
        else:
            print(f"    > {cmd_name} Ok.")

    def execute_query(self, query_name, kwargs):
        print("QUERY:", query_name, kwargs)
        try:
            meth = getattr(self._backend_store, query_name)
        except AttributeError:
            print(f" < {query_name} ERROR: unknown query")
            return
        try:
            result = meth(**kwargs)
        except Exception as err:
            print(f" < {query_name} ERROR:", err)
        else:
            print(f" < QUERY {query_name} Result: {result}")
        return result


class ClientBroker:
    """
    Abstract utility class managing the nats connection.
    Inherit this if you need to connect using an existing nats client.
    """

    def __init__(self, stream_name: str, subject_prefix: str):
        self._stream_name = stream_name
        self._subject_prefix = subject_prefix

    async def connect(self, **kargs) -> None: ...
    async def disconnect(self) -> None: ...
    async def send_cmd(self, cmd_name, **kwargs) -> None: ...
    async def send_query(self, query_name: str, **kwargs) -> Any: ...
    async def _on_touch_event(self, event) -> None: ...


class JetStreamClientBroker(ClientBroker):
    """
    A ClientBroker creating its own nats connection during its `connect(...)` call.
    """

    def __init__(self, stream_name: str, subject_prefix: str):
        super().__init__(stream_name=stream_name, subject_prefix=subject_prefix)
        self._cmd_subject_prefix = f"{subject_prefix}.$CMD."
        self._request_subject_prefix = f"{subject_prefix}.$QUERY."
        self._touch_event_prefix = f"{subject_prefix}.$EVENT."

        self._touch_subscription = None

    async def connect(self, servers: str | list[str], user_credentials: str):
        try:
            nc = await nats.connect(
                servers,
                user_credentials=user_credentials,
                name="contextual_settings_js_client",
            )
        except Exception as err:
            print("Could not connect, Aborting because:", err)
            return
        else:
            self._nc = nc
            self._js = nc.jetstream()
            self._touch_subscription = await self._nc.subscribe(
                self._touch_event_prefix + ">", cb=self._on_touch_event
            )

    async def disconnect(self):
        if self._touch_subscription is not None:
            await self._touch_subscription.unsubscribe()
        await self._nc.drain()

    async def send_cmd(self, cmd_name, **kwargs) -> None:
        subject = self._cmd_subject_prefix + cmd_name
        payload = json.dumps(kwargs)
        ack = await self._js.publish(
            subject, payload.encode(), stream=self._stream_name
        )
        print("CMD SENT", ack)

    async def send_query(self, query_name: str, **kwargs) -> Any:
        subject = self._request_subject_prefix + query_name
        payload = json.dumps(kwargs)
        response = await self._nc.request(subject, payload.encode(), timeout=0.5)
        data = json.loads(response.data.decode())
        print("[QUERY SENT]", query_name, kwargs, "@", subject, "->", response)
        return data

    async def _on_touch_event(self, msg) -> None:
        print("Got touch Event:", msg)


class JetStreamStoreClient(BaseStore):
    def __init__(self, broker: ClientBroker):
        self._broker = broker

    async def connect(self, *args, **kwargs) -> None:
        await self._broker.connect(*args, **kwargs)

    async def disconnect(self) -> None:
        await self._broker.disconnect()

    async def _send_cmd(self, cmd_name, **kwargs) -> None:
        await self._broker.send_cmd(cmd_name, **kwargs)

    async def _send_query(self, query_name: str, **kwargs) -> Any:
        return await self._broker.send_query(query_name=query_name, **kwargs)

    # ---

    async def _resolve_flat(
        self, contexts: list[str], with_history: bool = False
    ) -> dict[str, Any]:
        data = await self._send_query(
            "_resolve_flat", contexts=contexts, with_history=with_history
        )
        return data  # type: ignore

    async def get_context_flat(
        self,
        context: list[str],
        path: str | None = None,
        with_history: bool = False,
    ) -> dict[str, Any]:
        values = await self._resolve_flat(expand_context_names(context), with_history)
        # FIXME: reduce string template in all flat values here!
        return self._build_context_flat(values, path, with_history)

    async def _resolve_context_data(
        self, contexts: list[str], with_history: bool = False
    ) -> ContextData:
        data = await self._send_query(
            "_resolve_context_data", contexts=contexts, with_history=with_history
        )
        context_data = ContextData.from_dict(data)
        return context_data

    async def get_context_dict(
        self, context: list[str], path: str | None = None, with_history: bool = False
    ) -> dict[str, Any]:
        values = await self._resolve_context_data(
            expand_context_names(context), with_history
        )
        # FIXME: reduce string template in all values here!
        return self._build_context_dict(values, path, with_history)

    async def get_context(
        self,
        context: list[str],
        model_type: type[ModelType],
        path: str | None = None,
    ) -> ModelType:
        t = time.time()
        values = await self._resolve_context_data(expand_context_names(context))
        # FIXME: reduce string template in all values here!
        logger.debug(f"->COMPUTED CONTEXT IN {time.time()-t:.5f}")
        return self._build_context(values, model_type, path)

    # ---

    async def get_context_names(self) -> tuple[str, ...]:
        data = await self._send_query("get_context_names")
        return data  # type: ignore
        # return tuple(self._context_ops.keys())

    async def set_context_info(self, context_name: str, **kwargs) -> None:
        await self._send_cmd("set_context_info", context_name=context_name, **kwargs)
        # self._context_info[context_name].update(kwargs)

    async def get_context_info(self, context_name: str) -> dict[str, Any]:
        data = await self._send_query("get_context_info", context_name=context_name)
        return data
        # return self._context_info[context_name]

    # ---

    async def update_context(
        self,
        context_name: str,
        model: ModelType,
        path: str | None = None,
        exclude_defaults: bool = True,
    ):
        # NB: this is a copy of super().update_context() with await on update_context_dict()
        # It the base implementation changes, this one must be updated accordingly!
        deep_dict = model.model_dump(exclude_defaults=exclude_defaults)
        await self.update_context_dict(context_name, deep_dict, path)

    async def update_context_dict(
        self,
        context_name: str,
        deep_dict: dict[str, Any | dict[str, Any]],
        path: str | None = None,
    ) -> None:
        return await self._send_cmd(
            "update_context_dict",
            context_name=context_name,
            deep_dict=deep_dict,
            path=path,
        )

    async def update_context_flat(
        self, context_name: str, flat_dict: dict[str, Any], path: str | None = None
    ) -> None:
        return await self._send_cmd(
            "update_context_flat",
            context_name=context_name,
            flat_dict=flat_dict,
            path=path,
        )

    #
    # ---
    #

    async def set(self, context_name: str, name: str, value: Any) -> None:
        await self._send_cmd("set", context_name=context_name, name=name, value=value)

    async def toggle(self, context_name: str, name: str) -> None:
        await self._send_cmd("toggle", context_name=context_name, name=name)

    async def add(self, context_name: str, name: str, value: Any) -> None:
        await self._send_cmd("add", context_name=context_name, name=name, value=value)

    async def sub(self, context_name: str, name: str, value: Any) -> None:
        await self._send_cmd("sub", context_name=context_name, name=name, value=value)

    async def set_item(
        self, context_name: str, name: str, index: int, item_value: Any
    ) -> None:
        await self._send_cmd(
            "set_item",
            context_name=context_name,
            name=name,
            index=index,
            item_value=item_value,
        )

    async def del_item(self, context_name: str, name: str, index: int) -> None:
        await self._send_cmd(
            "del_item", context_name=context_name, name=name, index=index
        )

    async def remove(self, context_name: str, name: str, item: str) -> None:
        await self._send_cmd("remove", context_name=context_name, name=name, item=item)

    async def append(self, context_name: str, name: str, value: Any) -> None:
        await self._send_cmd(
            "append", context_name=context_name, name=name, value=value
        )

    async def env_override(
        self, context_name: str, name: str, envvar_name: str
    ) -> None:
        """Set the value from the given env var only if that env var exists."""
        await self._send_cmd(
            "env_override",
            context_name=context_name,
            name=name,
            envvar_name=envvar_name,
        )

    async def pop(self, context_name: str, name: str, index: int | slice) -> None:
        await self._send_cmd("pop", context_name=context_name, name=name, index=index)

    async def remove_slice(
        self,
        context_name: str,
        name: str,
        start: int,
        stop: int,
        step: int | None = None,
    ) -> None:
        await self._send_cmd(
            "remove_slice",
            context_name=context_name,
            name=name,
            start=start,
            stop=stop,
            step=step,
        )

    async def call(
        self,
        context_name: str,
        name: str,
        method_name: str,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> None:
        await self._send_cmd(
            "call",
            context_name=context_name,
            name=name,
            method_name=method_name,
            args=args,
            kwargs=kwargs,
        )


async def run_service_forever(
    nats_endpoint: str | None,
    secret_cred: str | None,
    stream_name: str | None = None,
    subject_prefix: str | None = None,
):

    nats_endpoint = nats_endpoint or os.environ.get("CSETTINGS_JS_URL")
    if nats_endpoint is None:
        raise ValueError(
            "Missing value for 'nats_endpoint' argument or 'CSETTINGS_JS_URL' env var!"
        )

    secret_cred = secret_cred or os.environ.get("CSETTINGS_JS_CREDS")
    if secret_cred is None:
        raise ValueError(
            "Missing value for 'secret_cred' argument or 'CSETTINGS_JS_CREDS' env var!"
        )

    stream_name = stream_name or os.environ.get("CSETTINGS_JS_STREAM")
    if stream_name is None:
        raise ValueError(
            "Missing value for 'stream_name' argument or 'CSETTINGS_JS_STREAM' env var!"
        )

    subject_prefix = subject_prefix or os.environ.get("CSETTINGS_JS_SUBJECT")
    if subject_prefix is None:
        raise ValueError(
            "Missing value for 'subject_prefix' argument or 'CSETTINGS_JS_SUBJECT' env var!"
        )

    if "---BEGIN NATS" in secret_cred:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".creds") as f:
            f.write(secret_cred)
            creds_path = f.name
            del_creds_path = True
    else:
        creds_path = secret_cred
        del_creds_path = False

    try:
        service_name = "contextual_settings_js_service"
        nc = await nats.connect(
            nats_endpoint,
            user_credentials=creds_path,
            name=service_name,
        )
    finally:
        if del_creds_path:
            os.unlink(creds_path)

    service = JetStreamStoreService(
        nc, stream_name=stream_name, subject_prefix=subject_prefix
    )
    connected = await service.connect()
    if not connected:
        print("Connection failed, aborting...")
        await nc.drain()
        return

    alive = True
    while alive:
        try:
            await asyncio.sleep(60)
            # print("Alive:", alive)
        except (Exception, KeyboardInterrupt, asyncio.exceptions.CancelledError) as err:
            print("!!!", err)
            alive = False

    print("Stopping service")
    await service.disconnet()
    print("Stopping nc")
    await nc.close()


def start_service(
    nats_endpoint: str | None = None,
    secret_cred: str | None = None,
    stream_name: str | None = None,
    subject_prefix: str | None = None,
):
    asyncio.run(
        run_service_forever(
            nats_endpoint=nats_endpoint,
            secret_cred=secret_cred,
            stream_name=stream_name,
            subject_prefix=subject_prefix,
        )
    )


async def test_client(
    nats_endpoint: str,
    secret_cred: str,
    stream_name: str,
    subject_prefix: str,
):
    import rich

    broker = JetStreamClientBroker(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
    )
    client = JetStreamStoreClient(broker)
    await client.connect(servers=nats_endpoint, user_credentials=secret_cred)

    # toggle these to test situations:
    WRITE = False
    READ = True

    if 0:
        if WRITE:
            await client.set_context_info("test_context", color="red")
        if READ:
            context_info = await client.get_context_info("test_context")
            print("--> context info", context_info)

        if WRITE:
            await client.set("my_context", "my_key", "my_value 2")
        if READ:
            context = await client.get_context_flat(["my_context"])
            print("--> flat context:", context)

        if READ:
            context = await client.get_context_dict(["my_context"], with_history=True)
            rich.print("--> dict context w/history:", context)

    if 1:
        import pydantic

        class MySettings(pydantic.BaseModel):
            value_str: str | None = None
            value_int: int = 0

        if WRITE:
            my_settings = MySettings(value_str="Yolo!", value_int=9)
            await client.update_context(
                "my_context", my_settings, "my_settings_key", exclude_defaults=False
            )
        if READ:
            context = await client.get_context(
                ["my_context"], MySettings, "my_settings_key"
            )
            rich.print("--> dict context w/history:", context)

    print("Stopping")
    await client.disconnect()


def start_test_client(
    nats_endpoint: str,
    secret_cred: str,
    stream_name: str,
    subject_prefix: str,
):
    asyncio.run(
        test_client(
            nats_endpoint,
            secret_cred,
            stream_name,
            subject_prefix,
        )
    )


if __name__ == "__main__":
    import sys

    if sys.argv[-1] == "service":
        start_service(
            nats_endpoint="tls://connect.ngs.global",
            secret_cred="/tmp/test.creds",
            stream_name="dev_settings",
            subject_prefix="dev.settings.proto",
        )
    elif sys.argv[-1] == "client":
        start_test_client(
            nats_endpoint="tls://connect.ngs.global",
            secret_cred="/tmp/test.creds",
            stream_name="dev_settings",
            subject_prefix="dev.settings.proto",
        )
    else:
        print("Bro.... -___-'")
