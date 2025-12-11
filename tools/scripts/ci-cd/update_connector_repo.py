import os
import sys
import json
import pathlib
import logging

from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

THINGSBOARD_URL = os.getenv("TB_URL")
USERNAME = os.getenv("TB_USER")
PASSWORD = os.getenv("TB_PASS")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD]):
    logging.error("TB_URL, TB_USER or TB_PASS not configured.")
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
        return {}

    connectors = {}
    for f in connectors_dir.glob("*.json"):
        with open(f, "r") as fp:
            cfg = json.load(fp)
        name = f.stem
        connectors[name] = {
            "mode": "advanced",
            "name": name,
            "type": detect_type_from_name(name),
            "logLevel": "INFO",
            "sendDataOnlyOnChange": False,
            "configurationJson": cfg
        }
    return connectors


def sync_gateway(client: RestClientPE, gateway_name: str):
    logging.info(f"Sync gateway: {gateway_name}")

    base = pathlib.Path("infra/thingsboard-gateway")
    matches = list(base.rglob(gateway_name))
    if not matches:
        logging.warning(f"Gateway folder '{gateway_name}' not found in repo.")
        return

    gw_path = matches[0]
    connectors = load_connectors_from_repo(gw_path)
    active_list = list(connectors.keys())
    payload = {"active_connectors": active_list, **connectors}

    try:
        device = client.get_tenant_device(gateway_name)
    except ApiException:
        logging.exception(f"API error fetching gateway '{gateway_name}'")
        return
    except Exception:
        logging.exception(f"Unexpected error fetching gateway '{gateway_name}'")
        return

    device_id = device.id.id

    try:
        current_attrs = client.get_device_attributes(device.id.id, scope="SHARED_SCOPE")
        current_keys = {attr.key for attr in current_attrs}
    except ApiException:
        logging.warning(f"API error loading attributes for '{gateway_name}', assuming empty.")
        current_keys = set()
    except Exception:
        logging.exception(f"Unexpected error loading attributes for '{gateway_name}'")
        current_keys = set()

    new_keys = set(payload.keys())
    keys_to_delete = list(current_keys - new_keys)

    if keys_to_delete:
        logging.info(f"Removing old connectors: {keys_to_delete}")
        try:
            client.delete_entity_attributes(
                entity_type="DEVICE",
                entity_id=device_id,
                scope="SHARED_SCOPE",
                keys=keys_to_delete
            )
        except ApiException:
            logging.exception(f"API error deleting attributes from '{gateway_name}'")
        except Exception:
            logging.exception(f"Unexpected error deleting attributes from '{gateway_name}'")

    try:
        client.save_device_attributes(
            device_id=device.id,
            scope="SHARED_SCOPE",
            body=payload
        )
        logging.info(f"Gateway '{gateway_name}' synced successfully.")
    except ApiException:
        logging.exception(f"API error updating '{gateway_name}'")
    except Exception:
        logging.exception(f"Unexpected error updating '{gateway_name}'")


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
    except ApiException:
        logging.exception("API login failed")
        sys.exit(1)
    except Exception:
        logging.exception("Unexpected error during login")
        sys.exit(1)

    pairs = list(zip(args[0::2], args[1::2]))
    gateways = {pathlib.Path(path).parent.parent.name for status, path in pairs}

    for gw in gateways:
        sync_gateway(client, gw)

    logging.info("Done.")
