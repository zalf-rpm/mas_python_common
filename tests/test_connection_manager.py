from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import capnp
from mas.schema.persistence import persistence_capnp
from mas.schema.test import a_capnp

from zalfmas_common.common import ConnectionManager, Restorer


class _Echo(a_capnp.A.Server):
    """Trivial test capability: echoes `param` back as `res`."""

    async def method(self, param, _context, **kwargs):
        _context.results.res = param


@asynccontextmanager
async def _running_restorer(port: int = 0) -> AsyncIterator[Restorer]:
    """Start a real local TwoPartyServer, backed by a fresh Restorer, on `port` (0 = ephemeral)."""
    restorer = Restorer()
    restorer.host = "127.0.0.1"

    async def new_connection(stream):
        await capnp.TwoPartyServer(stream, bootstrap=restorer).on_disconnect()

    server = await capnp.AsyncIoStream.create_server(new_connection, restorer.host, port)
    restorer.port = server.sockets[0].getsockname()[1]
    try:
        yield restorer
    finally:
        # Deliberately not awaiting wait_closed(): its handler tasks are still parked on
        # on_disconnect() for any connection a ConnectionManager is (correctly) keeping alive,
        # so it would hang until the client side closes first. close() alone stops accepting
        # new connections, which is all teardown needs here - open connections are cleaned up
        # when the test's event loop closes at the end of asyncio.run().
        server.close()


def _sturdy_ref(restorer: Restorer, sr_token: str):
    return persistence_capnp.SturdyRef.new_message(**restorer.sturdy_ref(sr_token))


async def _echo_call(cap) -> str:
    return (await cap.method(param="ping")).res


def test_connect_and_call_a_capability() -> None:
    """Baseline smoke test: connect() can reach a real remote capability and call it."""

    async def run_test() -> None:
        async with _running_restorer() as restorer:
            sr_token, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager()

            cap = await con_man.connect(_sturdy_ref(restorer, sr_token), cast_as=a_capnp.A)

            assert cap is not None
            assert await _echo_call(cap) == "ping"

    asyncio.run(capnp.run(run_test()))


def test_connection_survives_gc_after_connect_returns() -> None:
    """Regression test for the premature-GC bug.

    A capability obtained via the sturdy-ref restore() path doesn't hold a reference back to
    the TwoPartyClient/socket that connect() created locally - so before the fix, once
    connect() returned and those locals went out of scope, the connection got garbage
    collected (and its socket closed) even though the caller was still actively using the
    capability. This reproduces that by forcing a collection right after connect() returns,
    before making the next call.

    Uses cache_connections=False explicitly: keeping a connection alive must not depend on
    whether it's also cached for reuse (caching being on can mask this bug by incidentally
    keeping a reference alive, which is exactly what happened historically).
    """

    async def run_test() -> None:
        async with _running_restorer() as restorer:
            sr_token, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager(cache_connections=False)

            cap = await con_man.connect(_sturdy_ref(restorer, sr_token), cast_as=a_capnp.A)
            assert cap is not None

            gc.collect()  # would previously collect the underlying TwoPartyClient/socket

            assert await _echo_call(cap) == "ping"

    asyncio.run(capnp.run(run_test()))


def test_cache_connections_true_reuses_one_underlying_connection() -> None:
    """With caching on (the default), two connects to the same vat share one TwoPartyClient."""

    async def run_test() -> None:
        async with _running_restorer() as restorer:
            sr_token_a, _ = await restorer.save_cap(_Echo())
            sr_token_b, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager(cache_connections=True)

            cap_a = await con_man.connect(_sturdy_ref(restorer, sr_token_a), cast_as=a_capnp.A)
            cap_b = await con_man.connect(_sturdy_ref(restorer, sr_token_b), cast_as=a_capnp.A)

            assert cap_a is not None
            assert cap_b is not None
            assert len(con_man._live_clients) == 1
            assert await _echo_call(cap_a) == "ping"
            assert await _echo_call(cap_b) == "ping"

    asyncio.run(capnp.run(run_test()))


def test_cache_connections_false_opens_a_new_connection_each_time() -> None:
    """With caching off, each connect() opens its own TwoPartyClient - but both still work,
    since keeping a connection alive must not depend on whether it's also cached for reuse.
    """

    async def run_test() -> None:
        async with _running_restorer() as restorer:
            sr_token_a, _ = await restorer.save_cap(_Echo())
            sr_token_b, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager(cache_connections=False)

            cap_a = await con_man.connect(_sturdy_ref(restorer, sr_token_a), cast_as=a_capnp.A)
            cap_b = await con_man.connect(_sturdy_ref(restorer, sr_token_b), cast_as=a_capnp.A)

            assert len(con_man._live_clients) == 2
            assert await _echo_call(cap_a) == "ping"
            assert await _echo_call(cap_b) == "ping"

    asyncio.run(capnp.run(run_test()))


def test_string_sturdy_ref_is_cached_by_vat_identity_not_host_port() -> None:
    """connect() also accepts sturdy refs as `capnp://` URL strings (the form used almost
    everywhere in production code). The resulting cache entry must be keyed by the vat's
    identity, not by host:port - a port can be reassigned by the OS to an unrelated vat once a
    connection closes, so a host:port-keyed cache could silently hand back the wrong peer's
    connection.
    """

    async def run_test() -> None:
        async with _running_restorer() as restorer:
            sr_token, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager(cache_connections=True)

            cap = await con_man.connect(restorer.sturdy_ref_str(sr_token), cast_as=a_capnp.A)

            assert cap is not None
            assert await _echo_call(cap) == "ping"
            assert restorer.base64_vat_id in con_man._connections
            assert f"{restorer.host}:{restorer.port}" not in con_man._connections

    asyncio.run(capnp.run(run_test()))


def test_reusing_a_port_for_a_different_vat_does_not_reuse_the_connection() -> None:
    """Regression test for the original host:port-keyed cache bug.

    If connections were cached by network address instead of vat identity, then once that
    address is closed and reused by an unrelated vat (which the OS is free to do), a cache hit
    could hand back a connection to the WRONG peer. Caching by vat identity avoids this by
    construction: connecting to vat B, which happens to reuse vat A's old port, must not reuse
    vat A's (closed) connection.
    """

    async def run_test() -> None:
        con_man = ConnectionManager(cache_connections=True)

        async with _running_restorer() as restorer_a:
            sr_token_a, _ = await restorer_a.save_cap(_Echo())
            cap_a = await con_man.connect(_sturdy_ref(restorer_a, sr_token_a), cast_as=a_capnp.A)
            assert await _echo_call(cap_a) == "ping"
            port = restorer_a.port
        # restorer_a's server is now closed; its port may be reassigned by the OS

        async with _running_restorer(port=port) as restorer_b:
            assert restorer_b.port == port  # same address vat A used, but a different vat
            sr_token_b, _ = await restorer_b.save_cap(_Echo())

            cap_b = await con_man.connect(_sturdy_ref(restorer_b, sr_token_b), cast_as=a_capnp.A)

            assert cap_b is not None
            assert await _echo_call(cap_b) == "ping"

    asyncio.run(capnp.run(run_test()))


def test_cache_entry_is_evicted_when_peer_disconnects() -> None:
    """A cache entry for a vat must not outlive the connection to it: once the peer
    disconnects, the entry must be dropped - otherwise a later caller would be handed a dead
    capability that fails on first use instead of transparently reconnecting, and a long-lived
    ConnectionManager would accumulate references to dead connections forever.
    """

    async def run_test() -> None:
        restorer = Restorer()
        restorer.host = "127.0.0.1"
        server_sides: list[capnp.TwoPartyServer] = []

        async def new_connection(stream):
            server = capnp.TwoPartyServer(stream, bootstrap=restorer)
            server_sides.append(server)
            await server.on_disconnect()

        server = await capnp.AsyncIoStream.create_server(new_connection, restorer.host, 0)
        restorer.port = server.sockets[0].getsockname()[1]
        try:
            sr_token, _ = await restorer.save_cap(_Echo())
            con_man = ConnectionManager(cache_connections=True)

            cap = await con_man.connect(_sturdy_ref(restorer, sr_token), cast_as=a_capnp.A)
            assert cap is not None
            assert restorer.base64_vat_id in con_man._connections
            assert len(con_man._live_clients) == 1

            # connect() always tries an SSL handshake first (see its docstring/implementation),
            # which our plain-text test server rejects immediately, so a first, short-lived
            # connection typically shows up here too, alongside the real one actually in use.
            for server_side in server_sides:
                server_side.close()  # simulate the peer(s) going away

            for _ in range(200):  # give the eviction task a chance to run
                if restorer.base64_vat_id not in con_man._connections:
                    break
                await asyncio.sleep(0.01)

            assert restorer.base64_vat_id not in con_man._connections
            assert len(con_man._live_clients) == 0
        finally:
            server.close()

    asyncio.run(capnp.run(run_test()))
