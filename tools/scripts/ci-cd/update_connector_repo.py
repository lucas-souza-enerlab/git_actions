import os
import sys
import json
import pathlib
import logging

from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException

# ---- CONFIGURAÇÃO DO LOGGING ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# -----------------------------------

THINGSBOARD_URL = os.getenv("TB_URL")
USERNAME = os.getenv("TB_USER")
PASSWORD = os.getenv("TB_PASS")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD]):
    logging.error("Variables TB_URL, TB_USER or TB_PASS are not configured.")
    sys.exit(1)


def detect_type_from_name(name: str) -> str:
    n = name.lower()
    if "modbus" in n:
        return "modbus"
    if "bacnet" in n:
        return "bacnet"
    return "custom"


def load_connectors_from_repo(gateway_folder: pathlib.Path) -> dict:
    connectors_dir = gateway_folder / "connectors"
    if not connectors_dir.exists():
        logging.warning("No connectors directory found at %s", connectors_dir)
        return {}

    connectors = {}

    for f in connectors_dir.glob("*.json"):
        try:
            with open(f, "r") as fp:
                cfg = json.load(fp)
        except (json.JSONDecodeError, OSError) as e:
            logging.error("Failed to read connector file %s: %s", f, e)
            continue

        name = f.stem
        connectors[name] = {
            "mode": "advanced",
            "name": name,
            "type": detect_type_from_name(name),
            "logLevel": "INFO",
            "sendDataOnlyOnChange": False,
            "configurationJson": cfg
        }

        logging.info("Loaded connector '%s' from %s", name, f)

    return connectors


def sync_gateway(client: RestClientPE, gateway_name: str):
    logging.info("=== Sync gateway: %s ===", gateway_name)

    base = pathlib.Path("infra/thingsboard-gateway")
    matches = list(base.rglob(gateway_name))
    if not matches:
        logging.error("Gateway folder '%s' not found in repo!", gateway_name)
        return

    gw_path = matches[0]
    connectors = load_connectors_from_repo(gw_path)
    active_list = list(connectors.keys())

    payload = {"active_connectors": active_list, **connectors}

    # --- Fetch gateway device ---
    try:
        device = client.get_tenant_device(gateway_name)
        logging.info("Gateway '%s' found in ThingsBoard.", gateway_name)
    except ApiException as e:
        logging.error("Error fetching gateway '%s': %s", gateway_name, e)
        return

    device_id = device.id.id

    # --- Fetch current attributes ---
    try:
        current_attrs = client.get_device_attributes(
            device_id=device.id,
            scope="SHARED_SCOPE"
        )
        current_keys = {attr.key for attr in current_attrs}
    except ApiException as e:
        logging.warning("Could not fetch current attributes for %s: %s", gateway_name, e)
        current_keys = set()

    new_keys = set(payload.keys())
    keys_to_delete = list(current_keys - new_keys)

    # --- Delete removed attributes ---
    if keys_to_delete:
        try:
            client.delete_entity_attributes(
                entity_type="DEVICE",
                entity_id=device_id,
                scope="SHARED_SCOPE",
                keys=keys_to_delete
            )
            logging.info("Removed deleted connectors: %s", keys_to_delete)
        except ApiException as e:
            logging.error("Error deleting attributes for '%s': %s", gateway_name, e)

    # --- Save updated attributes ---
    try:
        client.save_device_attributes(
            device_id=device.id,
            scope="SHARED_SCOPE",
            body=payload
        )
        logging.info("Gateway '%s' synced successfully.", gateway_name)
    except ApiException as e:
        logging.error("Error syncing '%s': %s", gateway_name, e)


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) < 2:
        logging.error("Usage: update_connector_repo.py <A|M path.json> <A|M path2.json> ...")
        sys.exit(1)

    logging.info("Connecting to ThingsBoard...")

    try:
        client = RestClientPE(base_url=THINGSBOARD_URL)
        client.login(username=USERNAME, password=PASSWORD)
        logging.info("Authenticated successfully.")
    except ApiException as e:
        logging.error("Login failed: %s", e)
        sys.exit(1)

    pairs = list(zip(args[0::2], args[1::2]))
    gateways = {pathlib.Path(path).parent.parent.name for status, path in pairs}

    for gw in gateways:
        sync_gateway(client, gw)

    logging.info("Done.")
