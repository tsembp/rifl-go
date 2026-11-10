"""Three-node bare-metal CloudLab profile for the RIFL RPC experiments."""

import geni.portal as portal
import geni.rspec.pg as pg

context = portal.Context()
context.defineParameter(
    "hardware_type",
    "Bare-metal node type",
    portal.ParameterType.STRING,
    "m510",
    longDescription="Use the same node type for every reported experiment.",
)
context.defineParameter(
    "disk_image",
    "Disk image URN",
    portal.ParameterType.STRING,
    "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD",
)
params = context.bindParameters()
request = context.makeRequestRSpec()

lan = request.LAN("experiment-lan")
lan.best_effort = True

for name, address in (
    ("server", "10.10.1.1"),
    ("client", "10.10.1.2"),
    ("controller", "10.10.1.3"),
):
    node = request.RawPC(name)
    node.hardware_type = params.hardware_type
    node.disk_image = params.disk_image
    interface = node.addInterface("experiment")
    interface.addAddress(pg.IPv4Address(address, "255.255.255.0"))
    lan.addInterface(interface)

context.printRequestRSpec()
