import os
import sys
import json
import pathlib
from collections import defaultdict

from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException


THINGSBOARD_URL = os.getenv("TB_URL")
USERNAME = os.getenv("TB_USER")
PASSWORD = os.getenv("TB_PASS")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD]):
    print("Error: variables TB_URL, TB_USER or TB_PASS not configured.")
    sys.exit(1)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def detect_type_from_name(name: str) -> str:
    """
    Simple heuristic to infer connector type from filename.
    """
    n = name.lower()
    if "modbus" in n:
        return "modbus"
    if "bacnet" in n:
        return "bacnet"
    return "custom"


def load_connectors_from_repo(gateway_folder: pathlib.Path) -> dict:
    """
    Loads all connector JSONs from:
        <gateway_folder>/connectors/*.json
    and builds the payload expected by ThingsBoard.
    """
    connectors_dir = gateway_folder / "connectors"

    if not connectors_dir.exists():
        print(f"No connectors folder found inside {gateway_folder}")
        return {}

    connectors = {}

    for f in connectors_dir.glob("*.json"):
        name = f.stem
        with open(f, "r") as fp:
            cfg = json.load(fp)

        connectors[name] = {
            "mode": "advanced",
            "name": name,
            "type": detect_type_from_name(name),
            "logLevel": "INFO",
            "sendDataOnlyOnChange": False,
            "configurationJson": cfg
        }

    return connectors


# ---------------------------------------------------------------------
# Sync Logic
# ---------------------------------------------------------------------

def sync_gateway(client: RestClientPE, gateway_name: str):
    """
    - Locates gateway folder in repo (infra/thingsboard/<gateway>)
    - Loads connectors
    - Sends all connectors + active list to ThingsBoard shared scope
    """
    print(f"\n=== Sync gateway: {gateway_name} ===")

    base = pathlib.Path("infra/thingsboard")
    matches = list(base.rglob(gateway_name))

    if not matches:
        print(f"Gateway folder '{gateway_name}' not found in repo!")
        return

    gw_path = matches[0]

    # Load connectors from repo
    connectors = load_connectors_from_repo(gw_path)
    active_list = list(connectors.keys())

    payload = {
        "active_connectors": active_list,
        **connectors
    }

    # -----------------------------------------------------------------
    # Fetch device from ThingsBoard
    # -----------------------------------------------------------------
    try:
        device = client.get_tenant_device(gateway_name)
    except ApiException as e:
        print(f"Error fetching gateway '{gateway_name}': {e}")
        return

    print(f" - Found device ID: {device.id.id}")
    print(f" - Sending {len(connectors)} connectors...")

    # -----------------------------------------------------------------
    # Save shared attributes
    # -----------------------------------------------------------------
    try:
        client.save_device_attributes(
            device_id=device.id,
            scope="SHARED_SCOPE",
            attributes=payload
        )
        print(f" ✓ Gateway '{gateway_name}' synced successfully.")
    except ApiException as e:
        print(f"Error syncing '{gateway_name}': {e}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":

    args = sys.argv[1:]

    if len(args) < 2:
        print("Usage: update_connector_repo.py <A|M path.json> <A|M path2.json> ...")
        sys.exit(1)

    print("Connecting to ThingsBoard...")
    client = RestClientPE(url=THINGSBOARD_URL)

    try:
        client.login(USERNAME, PASSWORD)
        print("Authenticated successfully.")
    except ApiException as e:
        print(f"Login failed: {e}")
        sys.exit(1)

    # Extract gateways from pairs (status, path)
    pairs = list(zip(args[0::2], args[1::2]))
    gateways = set()

    for status, path in pairs:
        p = pathlib.Path(path)
        # infer gateway name: <gateway>/connectors/file.json → parent.parent.name
        gateway = p.parent.parent.name
        gateways.add(gateway)

    # Sync each gateway touched by the diff
    for gw in gateways:
        sync_gateway(client, gw)

    print("\nDone.")
