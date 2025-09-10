# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import argparse
import asyncio
import os
import threading
from datetime import datetime

import capnp
import tomli as ti
import tomlkit as tk
from zalfmas_capnp_schemas import (
    persistence_capnp,
    service_capnp,
    storage_capnp,
)
from zalfmas_capnp_schemas import (
    registry_capnp as reg_capnp,
)

from zalfmas_common import common
from zalfmas_common.common import ConnectionManager


class AdministrableService:
    def __init__(self, admin=None):
        self._admin = admin

    @property
    def admin(self):
        return self._admin

    @admin.setter
    def admin(self, a):
        self._admin = a

    def refesh_timeout(self):
        if self.admin:
            self.admin.heartbeat_context(None)


class Admin(service_capnp.Admin.Server, common.Identifiable):
    def __init__(self, services, id=None, name=None, description=None, timeout=0):
        common.Identifiable.__init__(self, id, name, description)

        self._services = services
        self._timeout = timeout
        self._timeout_prom = None
        self.make_timeout()
        self._unreg_sturdy_refs = {}  # name to (unreg action, rereg sturdy ref)
        self._stop_action = None
        self._tasks = []

    @property
    def stop_action(self):
        return self._stop_action

    @stop_action.setter
    def stop_action(self, a):
        self._stop_action = a

    @property
    def tasks(self):
        return self._tasks

    def store_unreg_data(self, name, unreg_action, rereg_sr):
        self._unreg_sturdy_refs[name] = (unreg_action, rereg_sr)

    def make_timeout(self):
        if self._timeout > 0:
            self._timeout_prom = (
                capnp.getTimer()
                .after_delay(self._timeout * 10**9)
                .then(lambda: exit(0))
            )

    async def heartbeat(self, **kwargs):  # heartbeat @0 ();
        if self._timeout_prom:
            self._timeout_prom.cancel()
        self.make_timeout()

    async def setTimeout(self, seconds, **kwargs):  # setTimeout @1 (seconds :UInt64);
        self._timeout = max(0, seconds)
        self.make_timeout()

    async def stop(self, **kwargs):  # stop @2 ();
        def stop():
            capnp.remove_event_loop(ignore_errors=True)
            exit(0)

        if self._stop_action:
            print("Admin::stop message with stop_action")
            return self._stop_action().then(
                lambda proms: [proms, threading.Timer(5, stop).start()][0]
            )
        else:
            print("Admin::stop message without")
            threading.Timer(5, stop).start()

    async def identities(
        self, **kwargs
    ):  # identities @3 () -> (infos :List(Common.IdInformation));
        infos = []
        for s in self._services:
            infos.append({"id": s.id, "name": s.name, "description": s.description})
        return infos

    async def updateIdentity(
        self, oldId, newInfo, **kwargs
    ):  # updateIdentity @4 (oldId :Text, newInfo :Common.IdInformation);
        for s in self._services:
            if s.id == oldId:
                s.id = newInfo.id
                s.name = newInfo.name
                s.description = newInfo.description


async def register_services(
    con_man: ConnectionManager,
    name_to_service: dict,
    admin: Admin,
    registries: list[dict],
):
    for name, cap in name_to_service.items():
        for reg in registries:
            if len(reg.items()) == 0:
                continue
            try:
                reg_sr = reg["sturdy_ref"]
                reg_name = reg.get("name", "")
                reg_cat_id = reg.get("category_id", "")
                print(
                    "Trying to register service with name:",
                    reg_name,
                    "@ category:",
                    reg_cat_id,
                )
                registrar = await con_man.try_connect(
                    reg_sr, cast_as=reg_capnp.Registrar
                )
                if registrar:
                    r = await registrar.register(
                        cap=cap, regName=reg_name, categoryId=reg_cat_id
                    )
                    unreg_action = r.unreg
                    rereg_sr = r.reregSR
                    admin.store_unreg_data(name, unreg_action, rereg_sr)
                    print(
                        "Registered service",
                        name,
                        "in category '",
                        reg_cat_id,
                        "' as '",
                        reg_name,
                        "'.",
                    )
                else:
                    print("Couldn't connect to registrar at sturdy_ref:", reg_sr)
            except Exception as e:
                print(
                    "Error registering service name:",
                    name,
                    "using data:",
                    reg,
                    ". Exception:",
                    e,
                )


async def register_vat_at_resolvers(
    con_man: ConnectionManager, resolvers: list, admin: Admin
):
    for res in resolvers:
        try:
            sr = res["sturdy_ref"]
            print("Trying to register vat at resolver sturdy_ref:", sr)
            registrar = await con_man.try_connect(
                sr, cast_as=persistence_capnp.HostPortResolver.Registrar
            )
            if registrar:
                req = registrar.register_request()
                req.host = con_man.restorer.host
                req.port = con_man.restorer.port
                req.base64VatId = con_man.restorer.base64_vat_id
                req.identityProof = con_man.restorer.signature_of_vat_id()
                if "alias" in res:
                    req.alias = res["alias"]
                r = await req.send()
                print(r)
                # r = await registrar.register(cap=name_to_service[name], regName=service_name,
                #                             categoryId=alias)
                hb = r.heartbeat
                hb_int = r.secsHeartbeatInterval

                async def heartbeat():
                    while True:
                        await asyncio.sleep(hb_int)
                        print("beat", datetime.now())
                        await hb.beat()

                admin.tasks.append(asyncio.create_task(heartbeat()))
                # admin.store_unreg_data(name, unreg_action, rereg_sr)
                print(
                    "Registered at resolver vat with vat_id:",
                    con_man.restorer.base64_vat_id,
                    f"and alias {res['alias']}." if "alias" in res else ".",
                )
            else:
                print("Couldn't connect to registrar at sturdy_ref:", sr)
        except Exception as e:
            print("Error registering vat. Exception:", e)


async def init_and_run_service_from_config(
    config: dict,
    service,
    restorer: common.Restorer = None,
    con_man: common.ConnectionManager = None,
    run_before_enter_eventloop=None,
):
    cs = config["service"]
    cv = config["vat"]
    await init_and_run_service(
        name_to_service={"service": service},
        name_to_service_srs={"service": cs.get("fixed_sturdy_ref_token", None)},
        host=cv.get("host", None),
        port=cv.get("port", None),
        serve_bootstrap=cv.get("serve_bootstrap", True),
        registries=cs.get("registries", None),
        resolvers=cv.get("resolvers", None),
        con_man=con_man,
        restorer=restorer,
        restorer_container_sr=cv.get("restorer_container_sr", None),
    )


async def init_and_run_service(
    name_to_service,
    host: str = None,
    port: int = None,
    serve_bootstrap=True,
    restorer: common.Restorer = None,
    con_man: common.ConnectionManager = None,
    name_to_service_srs: dict = None,
    run_before_enter_eventloop=None,
    restorer_container_sr: str = None,
    registries: dict = None,
    resolvers: dict = None,
):
    registries = registries if registries else []
    resolvers = resolvers if resolvers else []

    if not restorer:
        restorer = common.Restorer()
    if not con_man:
        con_man = common.ConnectionManager(restorer)
    if not name_to_service_srs:
        name_to_service_srs = {}

    if restorer and restorer_container_sr:
        restorer_container = await con_man.try_connect(
            restorer_container_sr, cast_as=storage_capnp.Store.Container
        )
        if restorer_container:
            restorer.storage_container = restorer_container
            await restorer.init_vat_id_from_container()
            if not port:
                await restorer.init_port_from_container()
                port = restorer.port

    # create and register admin interface with services
    admin = Admin(list(name_to_service.values()))
    for s in name_to_service.values():
        if isinstance(s, AdministrableService):
            s.admin = admin
    if "admin" not in name_to_service and admin not in name_to_service.values():
        name_to_service["admin"] = admin

    async def new_connection(stream):
        await capnp.TwoPartyServer(stream, bootstrap=restorer).on_disconnect()

    port = port if port else 0
    if serve_bootstrap:
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        restorer.port = server.sockets[0].getsockname()[1]

        for name, s in name_to_service.items():
            res = await restorer.save_str(
                cap=s, fixed_sr_token=name_to_service_srs.get(name, None)
            )
            name_to_service_srs[name] = res["sturdy_ref"]
            print("service:", name, "sr:", res["sturdy_ref"])
        print("restorer_sr:", restorer.sturdy_ref_str())

        await register_services(con_man, name_to_service, admin, registries)
        await register_vat_at_resolvers(con_man, resolvers, admin)
        if run_before_enter_eventloop:
            run_before_enter_eventloop()
        async with server:
            await server.serve_forever()
    # else:
    #    await register_services(con_man, admin, reg_config)
    #    if run_before_enter_eventloop:
    #        run_before_enter_eventloop()
    #    await con_man.manage_forever()


def handle_default_service_args_with_dict(parser, config: dict = None):
    args = parser.parse_args()

    remove_keys = []
    doc = tk.document()
    doc.add(tk.comment(f"{parser.prog} Service configuration (data and documentation)"))
    defaults = tk.table()
    opts = tk.table()
    if config:
        for k, v in config.items():
            if v is None:
                continue
            if "opt:" in k:
                opts.add(k[4:], v)
                remove_keys.append(k)
            else:
                defaults.add(k, v)
    if len(defaults) > 0:
        doc.add("defaults", defaults)
    if len(opts) > 0:
        doc.add("options", opts)

    if args.output_toml_config:
        print(tk.dumps(doc))
        exit(0)
    elif args.write_toml_config:
        with open(args.write_toml_config, "w") as _:
            tk.dump(doc, _)
            exit(0)
    elif args.config_toml is not None:
        with open(args.config_toml) as f:
            toml_config = tk.load(f)
        config.update(
            {k: v for k, v in toml_config.items() if type(v) is not tk.items.Table}
        )
        if "vat" in toml_config:
            config.update(
                {
                    f"vat.{k}": v
                    for k, v in toml_config["vat"].items()
                    if type(v) is not tk.items.Table
                }
            )
    else:
        parser.error("argument config_toml: expected path to config TOML file")

    for k in remove_keys:
        del config[k]
    return config, args, toml_config


def handle_default_service_args(
    parser,
    path_to_template_config=None,
    path_to_service_py=None,
    relative_path_from_service_py_to_default_configs_folder="default_configs",
):
    args = parser.parse_args()
    # use or construct path to template configuration file
    if not path_to_template_config and path_to_service_py:
        path_to_template_config = os.path.join(
            os.path.dirname(path_to_service_py),
            relative_path_from_service_py_to_default_configs_folder,
            os.path.basename(path_to_service_py).replace(".py", ".toml"),
        )

    # if there is a template config, load and use that for default values
    if args.output_toml_config or args.write_toml_config:
        if path_to_template_config:
            with open(path_to_template_config) as f:
                toml_doc = tk.load(f)
            if args.output_toml_config:
                print(tk.dumps(toml_doc))
                exit(0)
            elif args.write_toml_config:
                with open(args.write_toml_config, "w") as _:
                    _.write(tk.dumps(toml_doc))
                    exit(0)
        else:
            print("No default configuration could be loaded.")
            exit(0)
    elif args.config_toml:
        config_dict = {}
        if path_to_template_config:
            with open(path_to_template_config, "rb") as f:
                config_dict = ti.load(f)
        with open(args.config_toml, "rb") as f:
            config_dict.update(ti.load(f))
        return config_dict, args
    else:
        parser.error("argument config_toml: expected path to config JSON file")

    return {}, args


def create_default_args_parser(
    component_description: str, default_config_path: str | None = None
):
    parser = argparse.ArgumentParser(description=component_description)
    parser.add_argument(
        "config_toml",
        type=str,
        nargs="?",
        help="TOML configuration file",
        default=default_config_path,
    )
    parser.add_argument(
        "--output_toml_config",
        "-o",
        action="store_true",
        help="Output TOML configuration file with default settings at commandline.",
    )
    parser.add_argument(
        "--write_toml_config",
        "-w",
        type=str,
        help="Create a TOML configuration file with default settings in the current directory.",
    )
    return parser
