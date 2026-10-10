import os
import json
import time
from datetime import datetime
import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

CLIENTS_SERVICE_URL = os.environ.get("CLIENTS_SERVICE_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "commandes-creees")

producer = None

def get_kafka_producer():
    """Initialise paresseusement le producteur Kafka avec mécanisme de retry."""
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"[COMMANDES-KAFKA] Connecté au broker {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
        except Exception as e:
            print(f"[COMMANDES-KAFKA] Impossible de se connecter à Kafka ({e})", flush=True)
            return None
    return producer

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200, "date": "2026-10-01"},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30, "date": "2026-10-02"},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80, "date": "2026-10-05"}
]

@app.route("/")
def home():
    return "Microservice Commandes (avec Kafka Producer)"

@app.get("/commandes")
def list_commandes():
    return jsonify(commandes)

@app.get("/commandes/<int:id>")
def get_commande(id):
    commande_trouvee = next((c for c in commandes if c["id"] == id), None)
    if not commande_trouvee:
        return jsonify(error="Commande Not Found"), 404

    client_id = commande_trouvee["client_id"]
    try:
        response = requests.get(f"{CLIENTS_SERVICE_URL}/clients/{client_id}", timeout=5)
        if response.status_code == 200:
            client_data = response.json()
        elif response.status_code == 404:
            client_data = None
        else:
            return jsonify(error="Erreur lors de la récupération du client"), 502
    except requests.exceptions.RequestException:
        return jsonify(
            error="Le microservice clients n'est pas disponible",
            commande=commande_trouvee
        ), 503

    resultat = {
        "id": commande_trouvee["id"],
        "produit": commande_trouvee["produit"],
        "montant": commande_trouvee["montant"],
        "client": client_data
    }
    return jsonify(resultat)

@app.post("/commandes")
def create_commande():
    """
    Crée une nouvelle commande et publie un événement dans Kafka :
    Topic: commandes-creees
    Format JSON:
    {
      "commande_id": 10,
      "client_id": 2,
      "produit": "PC",
      "montant": 1200,
      "date": "2026-10-10"
    }
    """
    data = request.get_json()
    if not data or "client_id" not in data or "produit" not in data or "montant" not in data:
        return jsonify(error="Champs obligatoires: client_id, produit, montant"), 400

    new_id = len(commandes) + 1 if not commandes else max(c["id"] for c in commandes) + 1
    today_str = data.get("date", datetime.utcnow().strftime("%Y-%m-%d"))

    nouvelle_commande = {
        "id": new_id,
        "client_id": int(data["client_id"]),
        "produit": str(data["produit"]),
        "montant": float(data["montant"]),
        "date": today_str
    }
    commandes.append(nouvelle_commande)

    # Préparation du message d'événement pour Kafka
    evenement = {
        "commande_id": nouvelle_commande["id"],
        "client_id": nouvelle_commande["client_id"],
        "produit": nouvelle_commande["produit"],
        "montant": nouvelle_commande["montant"],
        "date": nouvelle_commande["date"]
    }

    # Publication vers Kafka
    prod = get_kafka_producer()
    kafka_published = False
    if prod:
        try:
            prod.send(KAFKA_TOPIC, value=evenement)
            prod.flush()
            kafka_published = True
            print(f"[COMMANDES-KAFKA] Événement publié sur '{KAFKA_TOPIC}' : {evenement}", flush=True)
        except Exception as e:
            print(f"[COMMANDES-KAFKA] Erreur lors de l'envoi de l'événement ({e})", flush=True)

    return jsonify({
        "message": "Commande créée avec succès",
        "commande": nouvelle_commande,
        "kafka_event_published": kafka_published
    }), 201

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
