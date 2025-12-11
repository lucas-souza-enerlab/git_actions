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
PATH_URL = os.getenv("TB_GATEWAY_CONFIG_PATH")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD, PATH_URL]):
    logging.error("TB_URL, TB_USER, TB_PASS or TB_GATEWAY_CONFIG_PATH not configured.")
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

    base = pathlib.Path(PATH_URL)
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
        logging.error(f"API error fetching gateway '{gateway_name}'", exc_info=True)
        return
    except Exception:
        logging.error(f"Unexpected error fetching gateway '{gateway_name}'", exc_info=True)
        return

    device_id = device.id.id

    try:
        current_attrs = client.get_device_attributes(device_id)
        current_keys = {attr.key for attr in current_attrs}
    except ApiException:
        logging.warning(
            f"API error loading attributes for '{gateway_name}', assuming no existing connectors."
        )
        current_keys = set()
    except TypeError:
        logging.info(
            f"get_device_attributes signature not compatible for '{gateway_name}', "
            f"skipping existing attribute diff."
        )
        current_keys = set()
    except Exception:
        logging.error(f"Unexpected error loading attributes for '{gateway_name}'", exc_info=True)
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
            logging.error(f"API error deleting attributes from '{gateway_name}'", exc_info=True)
        except Exception:
            logging.error(
                f"Unexpected error deleting attributes from '{gateway_name}'", exc_info=True
            )

    try:
        client.save_device_attributes(
            device_id=device.id,
            scope="SHARED_SCOPE",
            body=payload
        )
        logging.info(f"Gateway '{gateway_name}' synced successfully.")
    except ApiException:
        logging.error(f"API error updating '{gateway_name}'", exc_info=True)
    except Exception:
        logging.error(f"Unexpected error updating '{gateway_name}'", exc_info=True)


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) < 2:
        logging.error(
            "Usage: sync-thingsboard-connectors.py <A|M path.json> <A|M path2.json> ..."
        )
        sys.exit(1)

    logging.info("Connecting to ThingsBoard...")

    try:
        client = RestClientPE(base_url=THINGSBOARD_URL)
        client.login(username=USERNAME, password=PASSWORD)
        logging.info("Authenticated successfully.")
    except ApiException:
        logging.error("API login failed", exc_info=True)
        sys.exit(1)
    except Exception:
        logging.error("Unexpected error during login", exc_info=True)
        sys.exit(1)

    pairs = list(zip(args[0::2], args[1::2]))
    gateways = {pathlib.Path(path).parent.parent.name for status, path in pairs}

    for gw in gateways:
        sync_gateway(client, gw)

    logging.info("Done.")
