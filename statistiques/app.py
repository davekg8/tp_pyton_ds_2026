import os
import json
import threading
import time
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "statistiques-group")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "commandes-creees")

# Données statistiques calculées à partir des événements Kafka
# 1. Total de commandes par produit : { produit: nombre_commandes }
commandes_par_produit = {}

# 2. Chiffre d'affaires par mois (format YYYY-MM) : { "2026-10": montant_total }
ca_par_mois = {}

def start_kafka_consumer():
    """Consommateur Kafka dédié aux statistiques (autre group_id)."""
    consumer = None
    while not consumer:
        try:
            print(f"[STATS-KAFKA] Connexion au broker {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[STATS-KAFKA] Connecté au topic '{KAFKA_TOPIC}' (groupe: {KAFKA_GROUP_ID})", flush=True)
        except Exception as e:
            print(f"[STATS-KAFKA] Erreur connexion Kafka ({e}), réessai dans 3s...", flush=True)
            time.sleep(3)

    for message in consumer:
        try:
            data = message.value
            produit = data.get("produit", "Inconnu")
            montant = float(data.get("montant", 0))
            date_str = data.get("date", "")
            # Extraction du mois YYYY-MM
            mois = date_str[:7] if len(date_str) >= 7 else "Non-défini"

            # 1. Nombre de commandes par produit
            commandes_par_produit[produit] = commandes_par_produit.get(produit, 0) + 1

            # 2. CA par mois
            ca_par_mois[mois] = round(ca_par_mois.get(mois, 0.0) + montant, 2)

            print(
                f"\n[STATS KAFKA EVENT TRAITÉ]\n"
                f"  -> Produit      : {produit} (Total: {commandes_par_produit[produit]})\n"
                f"  -> Montant      : {montant}€\n"
                f"  -> Mois         : {mois} (CA cumulé: {ca_par_mois[mois]}€)\n",
                flush=True
            )
        except Exception as err:
            print(f"[STATS-KAFKA] Erreur traitement événement : {err}", flush=True)

@app.route("/")
def home():
    return "Microservice Statistiques (Kafka Consumer)"

@app.get("/statistiques")
def get_stats():
    return jsonify({
        "commandes_par_produit": commandes_par_produit,
        "ca_par_mois": ca_par_mois
    })

@app.get("/statistiques/produit/<nom_produit>")
def get_produit_stat(nom_produit):
    count = commandes_par_produit.get(nom_produit, 0)
    return jsonify({
        "produit": nom_produit,
        "total_commandes": count
    })

@app.get("/statistiques/ca/<mois>")
def get_ca_mois(mois):
    ca = ca_par_mois.get(mois, 0.0)
    return jsonify({
        "mois": mois,
        "chiffre_affaires": ca
    })

if __name__ == "__main__":
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8080)
