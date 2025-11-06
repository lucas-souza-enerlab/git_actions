import requests
import json
import os
import sys
import pathlib


THINGSBOARD_URL = os.getenv("TB_URL")
USERNAME = os.getenv("TB_USER")
PASSWORD = os.getenv("TB_PASS")

if not all([THINGSBOARD_URL, USERNAME, PASSWORD]):
    print(" Erro: variáveis TB_URL, TB_USER ou TB_PASS não configuradas.")
    sys.exit(1)


def get_token():
    """Faz login e retorna o token JWT"""
    url = f"{THINGSBOARD_URL}/api/auth/login"
    payload = {"username": USERNAME, "password": PASSWORD}
    print(f" Autenticando em {THINGSBOARD_URL} ...")
    r = requests.post(url, json=payload)
    r.raise_for_status()
    token = r.json().get("token")
    print(" Token obtido com sucesso.")
    return token


def get_gateway_id(token, gateway_name):
    """Procura o gateway pelo nome e retorna o UUID"""
    url = f"{THINGSBOARD_URL}/api/tenant/devices?deviceName={gateway_name}"
    headers = {"X-Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f" Erro ao buscar gateway '{gateway_name}': {r.status_code}")
        print(r.text)
        sys.exit(1)

    data = r.json()
    if "id" in data and "id" in data["id"]:
        device_id = data["id"]["id"]
        print(f" Gateway '{gateway_name}' encontrado com ID: {device_id}")
        return device_id
    else:
        print(f" Gateway '{gateway_name}' não encontrado.")
        sys.exit(1)


def get_current_shared_scope(token, device_id):
    """Busca o JSON atual do SHARED_SCOPE"""
    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/SHARED_SCOPE"
    headers = {"X-Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f" Erro ao buscar shared scope: {r.status_code}")
        return {}

    try:
        data = r.json()
        for attr in data:
            if attr["key"] == "shared":
                return json.loads(attr["value"])
        return {}
    except Exception as e:
        print(f" Erro ao decodificar shared scope: {e}")
        return {}


def build_connector_payload(connector_name, connector_json):
    """Cria o JSON completo no formato ThingsBoard"""
    connector_name_upper = connector_name

    # Detecta tipo (heurística simples)
    if "port" in json.dumps(connector_json).lower():
        connector_type = "modbus"
    elif "objectIdentifier" in json.dumps(connector_json):
        connector_type = "bacnet"
    else:
        connector_type = "custom"

    payload = {
        connector_name_upper: {
            "mode": "advanced",
            "name": connector_name_upper,
            "type": connector_type,
            "logLevel": "DEBUG",
            "sendDataOnlyOnChange": False,
            "configurationJson": connector_json
        }
    }
    return payload


def update_connector(token, device_id, gateway_name, connector_name, connector_json):
    """Atualiza o conector com merge seguro"""
    print(f"\n Buscando configuração atual de '{gateway_name}' ...")
    current_shared = get_current_shared_scope(token, device_id)

    formatted_payload = build_connector_payload(connector_name, connector_json)
    print(f" Formatando conector '{connector_name}' no padrão ThingsBoard...")

    # Merge — mantém os outros conectores existentes
    current_shared.update(formatted_payload)

    url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/SHARED_SCOPE"
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}"
    }

    print(f" Enviando atualização do conector '{connector_name}' ...")
    r = requests.post(url, headers=headers, data=json.dumps(current_shared))
    if r.status_code == 200:
        print(f" Conector '{connector_name}' atualizado com sucesso no gateway {gateway_name}.")
    else:
        print(f" Falha ao atualizar conector: {r.status_code}")
        print(r.text)


def infer_from_path(path):
    """Extrai gateway e conector a partir do caminho do arquivo"""
    p = pathlib.Path(path)
    gateway_name = p.parent.parent.name
    connector_name = p.stem()
    with open(path, "r") as f:
        connector_json = json.load(f)
    return gateway_name, connector_name, connector_json


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python update_connector_repo.py <arquivo.json> [<arquivo2.json> ...]")
        sys.exit(1)

    token = get_token()

    for path in sys.argv[1:]:
        gateway_name, connector_name, connector_json = infer_from_path(path)
        print(f"\n Atualizando gateway '{gateway_name}' | conector '{connector_name}' | arquivo '{path}'")
        device_id = get_gateway_id(token, gateway_name)
        update_connector(token, device_id, gateway_name, connector_name, connector_json)
