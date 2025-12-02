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
    payload = {"username": USERNAME, "password": PASSWORD}
    print(f"Autenticando em {THINGSBOARD_URL} ...")
    r = requests.post(url, json=payload, timeout=15)
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
        print("Erro ao buscar shared scope, usando {}")
        return {}

    try:
        data = r.json()
        for attr in data:
            if attr["key"] == "shared":
                # Pode vir string JSON, então decodifica
                if isinstance(attr["value"], str):
                    return json.loads(attr["value"])
                return attr["value"]
        return {}
    except:
        return {}




def build_connector_payload(connector_name, connector_json):
    name_lower = connector_name.lower()

    if "modbus" in name_lower:
        connector_type = "modbus"
    elif "bacnet" in name_lower:
        connector_type = "bacnet"
    else:
        connector_type = "custom"

    payload = {
        connector_name: {
            "mode": "advanced",
            "name": connector_name,
            "type": connector_type,
            "logLevel": "DEBUG",
            "sendDataOnlyOnChange": False,
            "configurationJson": connector_json
        }
    }

    return payload



def create_connector(token, device_id, gateway_name, connector_name, connector_json):
    print(f"\nCriando conector '{connector_name}' no gateway '{gateway_name}'...")

    current_shared = get_current_shared_scope(token, device_id)

    # 1️⃣ active_connectors
    active = current_shared.get("active_connectors", [])
    if connector_name not in active:
        active.append(connector_name)
    current_shared["active_connectors"] = active

    # 2️⃣ payload completo
    new_payload = build_connector_payload(connector_name, connector_json)

    # 3️⃣ merge final
    current_shared.update(new_payload)

    # 4️⃣ enviar
    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    r = requests.post(url, headers=headers, data=json.dumps(current_shared))

    if r.status_code == 200:
        print(f"Conector '{connector_name}' criado com sucesso!")
    else:
        print(f"Falha ao criar conector: {r.status_code}")
        print(r.text)




def update_connector(token, device_id, gateway_name, connector_name, connector_json):
    print(f"\nAtualizando conector '{connector_name}' no gateway '{gateway_name}'...")

    current_shared = get_current_shared_scope(token, device_id)

    formatted_payload = build_connector_payload(connector_name, connector_json)

    # merge mantendo os outros conectores
    current_shared.update(formatted_payload)

    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    r = requests.post(url, headers=headers, data=json.dumps(current_shared))

    if r.status_code == 200:
        print(f"Conector '{connector_name}' atualizado com sucesso!")
    else:
        print(f"Falha ao atualizar conector: {r.status_code}")
        print(r.text)




def infer_from_path(path):
    p = pathlib.Path(path)
    gateway_name = p.parent.parent.name
    connector_name = p.stem
    with open(path, "r") as f:
        connector_json = json.load(f)
    return gateway_name, connector_name, connector_json




if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python update_connector_repo.py <A|M caminho.json> ...")
        sys.exit(1)

    token = get_token()

    args = sys.argv[1:]
    pairs = list(zip(args[0::2], args[1::2]))

    for status, path in pairs:
        gateway_name, connector_name, connector_json = infer_from_path(path)
        device_id = get_gateway_id(token, gateway_name)

        if status == "A":
            create_connector(token, device_id, gateway_name, connector_name, connector_json)

        elif status == "M":
            update_connector(token, device_id, gateway_name, connector_name, connector_json)

        else:
            print(f"Ignorando status '{status}' para arquivo {path}")
