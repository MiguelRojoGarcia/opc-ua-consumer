"""Cliente de comprobación: recorre el espacio de direcciones, lee valores y se suscribe unos segundos.

Uso:  docker compose exec kepserver python check_client.py [endpoint] [segundos]
"""

import asyncio
import sys

from asyncua import Client, ua

ENDPOINT = sys.argv[1] if len(sys.argv) > 1 else "opc.tcp://localhost:49320"
SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 5


class Printer:
    def datachange_notification(self, node, val, data):
        print(f"  [sub] {node.nodeid.Identifier} = {val}")


async def walk(node, depth=0):
    for child in await node.get_children():
        if child.nodeid.NamespaceIndex == 0:
            continue
        cls = await child.read_node_class()
        name = child.nodeid.to_string()
        if cls == ua.NodeClass.Variable:
            print(f"{'  ' * depth}{name} = {await child.read_value()}")
        else:
            print(f"{'  ' * depth}{name}/")
            await walk(child, depth + 1)


async def main():
    async with Client(ENDPOINT) as client:
        idx = await client.get_namespace_index("KEPServerEX")
        print(f"conectado a {ENDPOINT}, namespace KEPServerEX = {idx}\n")
        await walk(client.nodes.objects)

        setpoint = client.get_node(f"ns={idx};s=Line1.PLC1.TempSetpoint")
        await setpoint.write_value(ua.Variant(78.5, ua.VariantType.Double))
        print(f"\nescrito TempSetpoint=78.5 -> leído {await setpoint.read_value()}")

        print(f"\nsuscripción durante {SECONDS}s:")
        sub = await client.create_subscription(500, Printer())
        await sub.subscribe_data_change([
            client.get_node(f"ns={idx};s=Line1.PLC1.Temperature"),
            client.get_node(f"ns={idx};s=Line1.PLC1.ProductionCount"),
        ])
        await asyncio.sleep(SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
