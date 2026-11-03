"""Simulador de KEPServerEX que publica métricas industriales por OPC-UA.

Replica las convenciones de KEPServerEX para que un cliente desarrollado contra
este simulador funcione igual contra un KEPServerEX real:
  - Endpoint en el puerto 49320.
  - Namespace "KEPServerEX" en el índice 2.
  - NodeIds string con la ruta completa: ns=2;s=Canal.Dispositivo[.Grupo].Tag
  - Tags _System por dispositivo (_Enabled, _Error, _NoError...).
"""

import asyncio
import logging
import math
import os
import random
import time
from datetime import datetime, timezone

import yaml
from asyncua import Server, ua

log = logging.getLogger("kepserver-sim")

VARIANT_TYPES = {
    "Boolean": ua.VariantType.Boolean,
    "Int16": ua.VariantType.Int16,
    "Int32": ua.VariantType.Int32,
    "Int64": ua.VariantType.Int64,
    "UInt16": ua.VariantType.UInt16,
    "UInt32": ua.VariantType.UInt32,
    "Float": ua.VariantType.Float,
    "Double": ua.VariantType.Double,
    "String": ua.VariantType.String,
}

INTEGER_TYPES = {"Int16", "Int32", "Int64", "UInt16", "UInt32"}


class Tag:
    def __init__(self, cfg, path):
        self.name = cfg["name"]
        self.type = cfg["type"]
        self.sim = cfg.get("sim", {"kind": "static", "value": 0})
        self.noise = float(cfg.get("noise", 0))
        self.writable = bool(cfg.get("writable", False))
        self.path = path
        self.node = None
        self.vtype = VARIANT_TYPES[self.type]
        self.value = self._coerce(self._initial_value())

    def _initial_value(self):
        kind = self.sim["kind"]
        if kind in ("counter", "random_walk"):
            return self.sim.get("start", self.sim.get("min", 0))
        if kind == "ramp":
            return self.sim.get("min", 0)
        if kind == "static":
            return self.sim.get("value", 0)
        if kind == "cycle":
            return self.sim["values"][0]
        if self.type == "Boolean":
            return False
        if self.type == "String":
            return ""
        return 0

    def next_value(self, elapsed, device_tags):
        s = self.sim
        kind = s["kind"]
        if kind == "sine":
            amp = (s["max"] - s["min"]) / 2
            v = s["min"] + amp + amp * math.sin(2 * math.pi * elapsed / s["period_s"])
        elif kind == "ramp":
            v = self.value + s.get("step", 1)
            if v > s["max"]:
                v = s["min"]
        elif kind == "random":
            v = random.uniform(s["min"], s["max"])
        elif kind == "random_walk":
            step = s.get("step", 1)
            v = min(s["max"], max(s["min"], self.value + random.uniform(-step, step)))
        elif kind == "counter":
            v = self.value + s.get("step", 1)
        elif kind == "toggle":
            v = int(elapsed // s["period_s"]) % 2 == 1
        elif kind == "cycle":
            values = s["values"]
            v = values[int(elapsed // s.get("period_s", 1)) % len(values)]
        elif kind == "threshold":
            v = device_tags[s["source"]].value > s["above"]
        elif kind == "static":
            v = self.value
        else:
            raise ValueError(f"simulación desconocida '{kind}' en {self.path}")

        if self.noise and kind not in ("counter", "static"):
            v += random.gauss(0, self.noise)
        return self._coerce(v)

    def _coerce(self, v):
        if self.type == "Boolean":
            return bool(v)
        if self.type == "String":
            return str(v)
        if self.type in INTEGER_TYPES:
            return int(round(v))
        return float(v)


async def add_system_tags(idx, device_node, prefix):
    path = f"{prefix}._System"
    folder = await device_node.add_folder(ua.NodeId(path, idx), ua.QualifiedName("_System", idx))
    for name, value, vtype in (
        ("_Enabled", True, ua.VariantType.Boolean),
        ("_Error", False, ua.VariantType.Boolean),
        ("_NoError", True, ua.VariantType.Boolean),
        ("_Simulated", True, ua.VariantType.Boolean),
        ("_DeviceId", prefix, ua.VariantType.String),
    ):
        await folder.add_variable(
            ua.NodeId(f"{path}.{name}", idx), ua.QualifiedName(name, idx), ua.Variant(value, vtype)
        )


async def add_tags(idx, parent, prefix, container, tags, device_tags):
    for tag_cfg in container.get("tags", []):
        path = f"{prefix}.{tag_cfg['name']}"
        if tag_cfg["type"] not in VARIANT_TYPES:
            raise ValueError(f"tipo '{tag_cfg['type']}' no soportado en {path}")
        tag = Tag(tag_cfg, path)
        tag.node = await parent.add_variable(
            ua.NodeId(path, idx),
            ua.QualifiedName(tag.name, idx),
            ua.Variant(tag.value, tag.vtype),
        )
        if tag.writable:
            await tag.node.set_writable()
        tags.append(tag)
        device_tags[tag.name] = tag

    for group in container.get("groups", []):
        path = f"{prefix}.{group['name']}"
        folder = await parent.add_folder(ua.NodeId(path, idx), ua.QualifiedName(group["name"], idx))
        await add_tags(idx, folder, path, group, tags, device_tags)


async def build_address_space(server, idx, config):
    tags = []
    objects = server.nodes.objects
    for channel in config["channels"]:
        ch_path = channel["name"]
        ch_node = await objects.add_folder(ua.NodeId(ch_path, idx), ua.QualifiedName(ch_path, idx))
        for device in channel["devices"]:
            dev_path = f"{ch_path}.{device['name']}"
            dev_node = await ch_node.add_folder(
                ua.NodeId(dev_path, idx), ua.QualifiedName(device["name"], idx)
            )
            await add_system_tags(idx, dev_node, dev_path)
            device_tags = {}
            await add_tags(idx, dev_node, dev_path, device, tags, device_tags)
            for t in device_tags.values():
                t.device_tags = device_tags
    return tags


async def simulate(tags, interval):
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        now = datetime.now(timezone.utc)
        for tag in tags:
            if tag.writable:
                # Respetar lo que haya escrito un cliente.
                tag.value = await tag.node.read_value()
            tag.value = tag.next_value(elapsed, tag.device_tags)
            await tag.node.write_value(
                ua.DataValue(
                    ua.Variant(tag.value, tag.vtype),
                    StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                    SourceTimestamp=now,
                    ServerTimestamp=now,
                )
            )
        await asyncio.sleep(interval)


async def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    config_path = os.getenv("CONFIG_PATH", "/config/tags.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    srv_cfg = config.get("server", {})

    server = Server()
    await server.init()
    server.set_endpoint(os.getenv("OPCUA_ENDPOINT", srv_cfg.get("endpoint", "opc.tcp://0.0.0.0:49320")))
    server.set_server_name(srv_cfg.get("name", "KEPServerEX Simulator"))
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    server.set_security_IDs(["Anonymous"])

    idx = await server.register_namespace(srv_cfg.get("namespace_uri", "KEPServerEX"))
    log.info("namespace '%s' registrado con índice %d", srv_cfg.get("namespace_uri", "KEPServerEX"), idx)
    tags = await build_address_space(server, idx, config)
    for t in tags:
        log.info("tag ns=%d;s=%s (%s%s)", idx, t.path, t.type, ", writable" if t.writable else "")

    interval = int(os.getenv("UPDATE_INTERVAL_MS", srv_cfg.get("update_interval_ms", 1000))) / 1000

    async with server:
        log.info("servidor OPC-UA escuchando en %s (actualización cada %.0f ms)", server.endpoint.geturl(), interval * 1000)
        await simulate(tags, interval)


if __name__ == "__main__":
    asyncio.run(main())
