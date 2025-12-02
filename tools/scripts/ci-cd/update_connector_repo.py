import requests
import json
import os
import sys
import pathlib
from collections import defaultdict

THINGSBOARD_URL = os.getenv("TB_URL")
USERNAME = os.getenv("TB_USER")
PASSWORD = os.getenv("TB_PASS")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD]):
    print("Erro: variáveis TB_URL, TB_USER ou TB_PASS não configuradas.")
    sys.exit(1)


def get_token():
    url = f"{THINGSBOARD_URL}/api/auth/login"
    r = requests.post(url, json={"username": USERNAME, "password": PASSWORD}, timeout=15)
    r.raise_for_status()
    return r.json().get("token")


def get_gateway_id(token, gateway_name):
    url = f"{THINGSBOARD_URL}/api/tenant/devices?deviceName={gateway_name}"
    headers = {"X-Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=15)

    if r.status_code != 200:
        print(f"Erro ao buscar gateway '{gateway_name}': {r.status_code}")
        sys.exit(1)

    data = r.json()
    if "id" in data and "id" in data["id"]:
        return data["id"]["id"]

    print(f"Gateway '{gateway_name}' não encontrado.")
    sys.exit(1)


def detect_type_from_name(name):
    n = name.lower()
    if "modbus" in n: return "modbus"
    if "bacnet" in n: return "bacnet"
    return "custom"


def load_connectors_from_repo(gateway_folder):
    connectors = {}
    for f in pathlib.Path(gateway_folder).glob("connectors/*.json"):
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


def sync_gateway(token, gateway_name):
    print(f"\nSincronizando gateway: {gateway_name}")

    gw_path = pathlib.Path("infra/thingsboard") / gateway_name
    if not gw_path.exists():
        print(f"Pasta do gateway '{gateway_name}' não encontrada no repo!")
        return

    connectors = load_connectors_from_repo(gw_path)

    active_list = list(connectors.keys())

    payload = {"active_connectors": active_list}
    payload.update(connectors)

    device_id = get_gateway_id(token, gateway_name)

    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"

    print(f" Enviando {len(connectors)} conectores para o ThingsBoard...")

    r = requests.post(url, headers=headers, data=json.dumps(payload))

    if r.status_code == 200:
        print(f"Gateway '{gateway_name}' sincronizado com sucesso!")
    else:
        print(f"Erro ao sincronizar gateway {gateway_name}:")
        print(r.status_code)
        print(r.text)


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) < 2:
        print("Uso: update_connector_repo.py <A|M caminho.json> ...")
        sys.exit(1)

    token = get_token()

    gateways = set()

    pairs = list(zip(args[0::2], args[1::2]))
    for status, path in pairs:
        p = pathlib.Path(path)
        gateway = p.parent.parent.name
        gateways.add(gateway)

    for gw in gateways:
        sync_gateway(token, gw)
