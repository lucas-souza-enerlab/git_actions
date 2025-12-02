import requests
import json
import os
import sys
import pathlib

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


def get_current_shared_scope(token, device_id):
    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/SHARED_SCOPE"
    headers = {"X-Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        return {}

    try:
        data = r.json()
        for attr in data:
            if attr["key"] == "shared":
                if isinstance(attr["value"], str):
                    return json.loads(attr["value"])
                return attr["value"]
        return {}
    except:
        return {}


def build_connector_payload(connector_name, connector_json):
    lname = connector_name.lower()
    if "modbus" in lname:
        ctype = "modbus"
    elif "bacnet" in lname:
        ctype = "bacnet"
    else:
        ctype = "custom"

    return {
        connector_name: {
            "mode": "advanced",
            "name": connector_name,
            "type": ctype,
            "logLevel": "INFO",
            "sendDataOnlyOnChange": False,
            "configurationJson": connector_json
        }
    }



def create_connector(token, device_id, gateway_name, connector_name, connector_json):
    print(f"\nCriando conector '{connector_name}' no gateway '{gateway_name}'...")

    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    shared = get_current_shared_scope(token, device_id)

    existing_connectors = [
        key for key in shared.keys()
        if isinstance(shared.get(key), dict)
        and "configurationJson" in shared.get(key, {})
    ]


    if connector_name not in existing_connectors:
        existing_connectors.append(connector_name)


    active_payload = {"active_connectors": existing_connectors}

    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"
    r = requests.post(url, headers=headers, data=json.dumps(active_payload))

    if r.status_code != 200:
        print("❌ Falha ao atualizar active_connectors:")
        print(r.text)
        return

    print("✔ active_connectors atualizado com sucesso.")

    connector_payload = build_connector_payload(connector_name, connector_json)

    r = requests.post(url, headers=headers, data=json.dumps(connector_payload))

    if r.status_code == 200:
        print(f"✔ Conector '{connector_name}' criado com sucesso!")
    else:
        print("❌ Falha ao criar conector:")
        print(r.text)



def update_connector(token, device_id, gateway_name, connector_name, connector_json):
    print(f"\nAtualizando conector '{connector_name}' no gateway '{gateway_name}'...")

    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    payload = build_connector_payload(connector_name, connector_json)

    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"
    r = requests.post(url, headers=headers, data=json.dumps(payload))

    if r.status_code == 200:
        print(f"✔ Conector '{connector_name}' atualizado com sucesso!")
    else:
        print("❌ Falha ao atualizar conector:")
        print(r.text)


def infer_from_path(path):
    p = pathlib.Path(path)
    gateway_name = p.parent.parent.name
    connector_name = p.stem
    with open(path, "r") as f:
        connector_json = json.load(f)
    return gateway_name, connector_name, connector_json



if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) < 2 or len(args) % 2 != 0:
        print("Uso: python update_connector_repo.py <A|M caminho.json> ...")
        sys.exit(1)

    token = get_token()

    pairs = list(zip(args[0::2], args[1::2]))

    for status, path in pairs:
        gateway, connector, cfg = infer_from_path(path)
        device_id = get_gateway_id(token, gateway)

        if status == "A":
            create_connector(token, device_id, gateway, connector, cfg)
        elif status == "M":
            update_connector(token, device_id, gateway, connector, cfg)
        else:
            print(f"Ignorando status {status} para {path}")
