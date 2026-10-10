import os
import json
import threading
import time
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "clients-group")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "commandes-creees")

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# Dictionnaire maintenant le nombre total de commandes effectuées par chaque client
# { client_id: total_commandes }
commandes_par_client = {}

def get_client_by_id(client_id):
    for c in clients:
        if c["id"] == client_id:
            return c
    return None

def start_kafka_consumer():
    """Consommateur Kafka exécuté dans un thread d'arrière-plan."""
    consumer = None
    # Attente avec reconnexion que Kafka soit démarré
    while not consumer:
        try:
            print(f"[CLIENTS-KAFKA] Connexion au broker {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[CLIENTS-KAFKA] Connecté avec succès au topic '{KAFKA_TOPIC}' (groupe: {KAFKA_GROUP_ID})", flush=True)
        except Exception as e:
            print(f"[CLIENTS-KAFKA] Erreur connexion Kafka ({e}), réessai dans 3s...", flush=True)
            time.sleep(3)

    for message in consumer:
        try:
            data = message.value
            client_id = data.get("client_id")
            produit = data.get("produit")
            montant = data.get("montant")
            commande_id = data.get("commande_id")

            # Récupération des informations du client
            client_obj = get_client_by_id(client_id)
            nom_client = client_obj["nom"] if client_obj else f"Inconnu (id={client_id})"

            # Maintien du nombre total de commandes par client
            commandes_par_client[client_id] = commandes_par_client.get(client_id, 0) + 1
            total = commandes_par_client[client_id]

            print(
                f"\n[EVENT KAFKA REÇU]\n"
                f"  -> Commande ID : {commande_id}\n"
                f"  -> Client      : {nom_client} (id={client_id})\n"
                f"  -> Produit     : {produit}\n"
                f"  -> Montant     : {montant}€\n"
                f"  -> Total commandes pour ce client : {total}\n",
                flush=True
            )
        except Exception as err:
            print(f"[CLIENTS-KAFKA] Erreur traitement message : {err}", flush=True)

@app.route("/")
def home():
    return "Microservice Clients (avec Kafka Consumer)"

@app.get("/clients")
def list_clients():
    return jsonify(clients)

@app.get("/clients/<int:id>")
def get_client(id):
    client = get_client_by_id(id)
    if client:
        return jsonify(client)
    return jsonify(error="Not Found"), 404

@app.get("/clients/<int:id>/commandes-stats")
def get_client_stats(id):
    client = get_client_by_id(id)
    if not client:
        return jsonify(error="Client Not Found"), 404
    total = commandes_par_client.get(id, 0)
    return jsonify({
        "client": client,
        "total_commandes": total
    })

if __name__ == "__main__":
    # Démarrage du consommateur Kafka dans un thread démon
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()

    app.run(host="0.0.0.0", port=8080)
