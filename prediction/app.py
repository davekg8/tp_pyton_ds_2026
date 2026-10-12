import os
import json
import threading
import time
import socket
import joblib
import pandas as pd
from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer

app = Flask(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "prediction-group")
KAFKA_TOPIC_REQUEST = os.environ.get("KAFKA_TOPIC_REQUEST", "demandes-predictions")
KAFKA_TOPIC_RESULT = os.environ.get("KAFKA_TOPIC_RESULT", "resultats-predictions")
MODEL_PATH = os.environ.get("MODEL_PATH", "modele_commande.pkl")
INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

# Chargement du modèle de Machine Learning
print(f"[{INSTANCE_ID}] Chargement du modèle depuis '{MODEL_PATH}'...", flush=True)
try:
    model = joblib.load(MODEL_PATH)
    print(f"[{INSTANCE_ID}] Modèle chargé avec succès : {type(model).__name__}", flush=True)
except Exception as e:
    print(f"[{INSTANCE_ID}] ERREUR lors du chargement du modèle : {e}", flush=True)
    model = None

# Métriques en mémoire
stats = {
    "total_predictions": 0,
    "predictions_positives": 0,
    "predictions_negatives": 0,
    "derniere_prediction": None
}
stats_lock = threading.Lock()

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
            print(f"[{INSTANCE_ID}] Producteur Kafka connecté à {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
        except Exception as e:
            print(f"[{INSTANCE_ID}] Impossible de connecter le producteur Kafka ({e})", flush=True)
            return None
    return producer

def faire_prediction(data):
    """
    Construit l'entrée DataFrame et effectue la prédiction + probabilité.
    """
    if model is None:
        raise RuntimeError("Modèle ML non disponible")

    # Construction de l'entrée conformément à la section 3.4 du TP3
    X = pd.DataFrame([{
        "age": float(data.get("age", 30)),
        "anciennete": float(data.get("anciennete", 1)),
        "nombre_commandes": float(data.get("nombre_commandes", 0)),
        "montant_total": float(data.get("montant_total", 0.0)),
        "montant_moyen": float(data.get("montant_moyen", 0.0)),
        "jours_depuis_derniere_commande": float(data.get("jours_depuis_derniere_commande", 30))
    }])

    # Prédiction et probabilité (section 3.5)
    prediction = int(model.predict(X)[0])
    probabilite = float(model.predict_proba(X)[0][1])

    with stats_lock:
        stats["total_predictions"] += 1
        if prediction == 1:
            stats["predictions_positives"] += 1
        else:
            stats["predictions_negatives"] += 1
        stats["derniere_prediction"] = {
            "client_id": data.get("client_id"),
            "prediction": prediction,
            "probabilite_commande": round(probabilite, 4),
            "timestamp": time.time()
        }

    return prediction, probabilite

def start_kafka_consumer():
    """
    Consommateur Kafka exécuté dans un thread d'arrière-plan.
    Consomme les demandes sur 'demandes-predictions', effectue la prédiction
    et publie le résultat sur 'resultats-predictions'.
    """
    consumer = None
    while not consumer:
        try:
            print(f"[{INSTANCE_ID}] Connexion au broker Kafka {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
            consumer = KafkaConsumer(
                KAFKA_TOPIC_REQUEST,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(
                f"[{INSTANCE_ID}] Consommateur Kafka connecté au topic '{KAFKA_TOPIC_REQUEST}' "
                f"(groupe: {KAFKA_GROUP_ID})",
                flush=True
            )
        except Exception as e:
            print(f"[{INSTANCE_ID}] Erreur connexion Kafka ({e}), réessai dans 3s...", flush=True)
            time.sleep(3)

    for message in consumer:
        try:
            data = message.value
            client_id = data.get("client_id")

            # Exécution de la prédiction
            prediction, probabilite = faire_prediction(data)

            # Affichage demandé à la section 3.5 du TP3
            print(f"\n[{INSTANCE_ID}] [DEMANDE PREDICTION RECUE]", flush=True)
            print("Client :", client_id, flush=True)
            print("Prediction :", prediction, flush=True)
            print("Probabilite :", round(probabilite, 4), flush=True)

            # Construction de l'événement de résultat (section 3.6)
            resultat = {
                "client_id": client_id,
                "prediction": int(prediction),
                "probabilite_commande": round(float(probabilite), 4),
                "instance_id": INSTANCE_ID
            }

            # Publication dans le topic de résultats
            prod = get_kafka_producer()
            if prod:
                prod.send(KAFKA_TOPIC_RESULT, value=resultat)
                prod.flush()
                print(f"[{INSTANCE_ID}] Résultat publié sur '{KAFKA_TOPIC_RESULT}' : {resultat}\n", flush=True)
            else:
                print(f"[{INSTANCE_ID}] Avertissement: producteur non prêt pour publier le résultat.", flush=True)

        except Exception as err:
            print(f"[{INSTANCE_ID}] Erreur lors du traitement de l'événement : {err}", flush=True)

@app.route("/")
def home():
    return jsonify({
        "service": "Microservice Prédiction",
        "description": "Classification ML du comportement d'achat client intégrée à Kafka",
        "instance": INSTANCE_ID,
        "modele_charge": model is not None
    })

@app.get("/health")
def health():
    return jsonify({
        "status": "healthy" if model is not None else "degraded",
        "instance": INSTANCE_ID,
        "model_loaded": model is not None,
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "consumer_group": KAFKA_GROUP_ID
    })

@app.post("/predict")
def api_predict():
    """
    Endpoint HTTP direct pour tester ou effectuer une prédiction synchrone.
    Exemple de payload :
    {
      "client_id": 25,
      "age": 32,
      "anciennete": 3,
      "nombre_commandes": 8,
      "montant_total": 1250,
      "montant_moyen": 156.25,
      "jours_depuis_derniere_commande": 12
    }
    """
    data = request.get_json()
    if not data:
        return jsonify(error="Données JSON requises"), 400

    try:
        prediction, probabilite = faire_prediction(data)
        resultat = {
            "client_id": data.get("client_id"),
            "prediction": prediction,
            "probabilite_commande": round(probabilite, 4),
            "instance_id": INSTANCE_ID
        }
        return jsonify(resultat)
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.get("/stats")
def get_stats():
    with stats_lock:
        return jsonify({
            "instance": INSTANCE_ID,
            "stats": stats
        })

if __name__ == "__main__":
    # Démarrage du consommateur Kafka en tâche de fond
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()

    app.run(host="0.0.0.0", port=8080)
