import os
import json
import time
import threading
import random
from datetime import datetime
import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

CLIENTS_SERVICE_URL = os.environ.get("CLIENTS_SERVICE_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_COMMANDES = os.environ.get("KAFKA_TOPIC", "commandes-creees")
KAFKA_TOPIC_PREDICTIONS_REQUEST = os.environ.get("KAFKA_TOPIC_PREDICTIONS_REQUEST", "demandes-predictions")
KAFKA_TOPIC_PREDICTIONS_RESULT = os.environ.get("KAFKA_TOPIC_PREDICTIONS_RESULT", "resultats-predictions")
KAFKA_GROUP_ID_PREDICTIONS = os.environ.get("KAFKA_GROUP_ID_PREDICTIONS", "commandes-predictions-group")

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

# Stockage en mémoire des derniers résultats de prédiction reçus par client
# { client_id: { "prediction": 1, "probabilite_commande": 0.84, ... } }
resultats_predictions = {}
resultats_lock = threading.Lock()

# Contrôle du simulateur de flux infini en arrière-plan
flux_simulation_running = False
flux_thread = None

def start_kafka_prediction_consumer():
    """
    Consommateur Kafka en arrière-plan pour recevoir les résultats de prédictions (TP3).
    Affiche le résultat de la prédiction comme demandé dans le sujet.
    """
    consumer = None
    while not consumer:
        try:
            print(f"[COMMANDES-KAFKA] Connexion au topic de résultats '{KAFKA_TOPIC_PREDICTIONS_RESULT}'...", flush=True)
            consumer = KafkaConsumer(
                KAFKA_TOPIC_PREDICTIONS_RESULT,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID_PREDICTIONS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[COMMANDES-KAFKA] Connecté avec succès au topic '{KAFKA_TOPIC_PREDICTIONS_RESULT}'", flush=True)
        except Exception as e:
            print(f"[COMMANDES-KAFKA] Erreur connexion Kafka ({e}), réessai dans 3s...", flush=True)
            time.sleep(3)

    for message in consumer:
        try:
            resultat = message.value
            client_id = resultat.get("client_id")
            pred = resultat.get("prediction")
            proba = resultat.get("probabilite_commande")
            instance_src = resultat.get("instance_id", "inconnue")

            with resultats_lock:
                resultats_predictions[client_id] = resultat

            # Affichage demandé à la section 3.6 du TP3
            label = "Passera commande (OUI)" if pred == 1 else "Ne passera pas commande (NON)"
            print(
                f"\n=======================================================\n"
                f"[COMMANDES-KAFKA] [RÉSULTAT DE PRÉDICTION REÇU]\n"
                f"  -> Client ID             : {client_id}\n"
                f"  -> Prédiction            : {pred} ({label})\n"
                f"  -> Probabilité commande  : {proba * 100:.2f}%\n"
                f"  -> Traité par instance   : {instance_src}\n"
                f"=======================================================\n",
                flush=True
            )
        except Exception as err:
            print(f"[COMMANDES-KAFKA] Erreur traitement résultat prédiction : {err}", flush=True)

@app.route("/")
def home():
    return "Microservice Commandes (avec Kafka Producer et Consumer de Prédiction)"

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
            prod.send(KAFKA_TOPIC_COMMANDES, value=evenement)
            prod.flush()
            kafka_published = True
            print(f"[COMMANDES-KAFKA] Événement publié sur '{KAFKA_TOPIC_COMMANDES}' : {evenement}", flush=True)
        except Exception as e:
            print(f"[COMMANDES-KAFKA] Erreur lors de l'envoi de l'événement ({e})", flush=True)

    return jsonify({
        "message": "Commande créée avec succès",
        "commande": nouvelle_commande,
        "kafka_event_published": kafka_published
    }), 201

# =========================================================================
# TP3 : Production de demandes de prédiction et consultation des résultats
# =========================================================================

def envoyer_demande_prediction_kafka(data_prediction):
    """Publie un événement de demande de prédiction dans Kafka."""
    prod = get_kafka_producer()
    if not prod:
        return False, "Broker Kafka non disponible"

    try:
        prod.send(KAFKA_TOPIC_PREDICTIONS_REQUEST, value=data_prediction)
        prod.flush()
        print(f"[COMMANDES-KAFKA] Demande prédiction publiée sur '{KAFKA_TOPIC_PREDICTIONS_REQUEST}' : {data_prediction}", flush=True)
        return True, "Publié avec succès"
    except Exception as e:
        print(f"[COMMANDES-KAFKA] Erreur publication demande prédiction ({e})", flush=True)
        return False, str(e)

@app.post("/commandes/demander-prediction")
def demander_prediction():
    """
    Section 3.3 du TP3 :
    Le microservice commandes produit un événement contenant les informations nécessaires à la prédiction.
    Format attendu :
    {
      "client_id": 25,
      "age": 32,
      "anciennete": 3,
      "nombre_commandes": 8,
      "montant_total": 1250,
      "montant_moyen": 156.25,
      "jours_depuis_derniere_commande": 12
    }
    Si certains champs sont omis, ils sont déduits ou complétés par défaut.
    """
    data = request.get_json() or {}
    client_id = data.get("client_id")
    if client_id is None:
        return jsonify(error="client_id est obligatoire"), 400

    # Déduction automatique à partir de l'historique des commandes si non spécifié
    cmds_client = [c for c in commandes if c["client_id"] == client_id]
    nb_cmds = len(cmds_client)
    total_m = sum(c["montant"] for c in cmds_client)
    m_moyen = round(total_m / nb_cmds, 2) if nb_cmds > 0 else 0.0

    evenement_prediction = {
        "client_id": int(client_id),
        "age": int(data.get("age", 30)),
        "anciennete": int(data.get("anciennete", 2)),
        "nombre_commandes": int(data.get("nombre_commandes", nb_cmds if nb_cmds > 0 else 1)),
        "montant_total": float(data.get("montant_total", total_m if total_m > 0 else 100.0)),
        "montant_moyen": float(data.get("montant_moyen", m_moyen if m_moyen > 0 else 100.0)),
        "jours_depuis_derniere_commande": int(data.get("jours_depuis_derniere_commande", 15))
    }

    succes, message = envoyer_demande_prediction_kafka(evenement_prediction)
    if not succes:
        return jsonify(error=message), 503

    return jsonify({
        "message": "Demande de prédiction envoyée à Kafka avec succès",
        "topic": KAFKA_TOPIC_PREDICTIONS_REQUEST,
        "evenement": evenement_prediction
    }), 202

@app.get("/commandes/predictions")
def list_predictions():
    """Retourne la liste des résultats de prédiction reçus par commandes."""
    with resultats_lock:
        return jsonify(resultats_predictions)

@app.get("/commandes/predictions/<int:client_id>")
def get_prediction_client(client_id):
    """Retourne le dernier résultat de prédiction reçu pour un client donné."""
    with resultats_lock:
        res = resultats_predictions.get(client_id)
        if res:
            return jsonify(res)
    return jsonify(error=f"Aucune prédiction reçue pour le client {client_id}"), 404

# =========================================================================
# TP3 Section 3.7 : Gestion du flux infini de demandes
# =========================================================================

def boucle_flux_infini(intervalle_secondes=1.0):
    global flux_simulation_running
    print(f"[FLUX-INFINI] Démarrage du flux de prédictions (intervalle: {intervalle_secondes}s)...", flush=True)
    c_id = 100
    while flux_simulation_running:
        data_alea = {
            "client_id": c_id,
            "age": random.randint(18, 68),
            "anciennete": random.randint(1, 10),
            "nombre_commandes": random.randint(1, 20),
            "montant_total": round(random.uniform(50, 2500), 2),
            "montant_moyen": round(random.uniform(20, 200), 2),
            "jours_depuis_derniere_commande": random.randint(1, 120)
        }
        envoyer_demande_prediction_kafka(data_alea)
        c_id += 1
        time.sleep(intervalle_secondes)
    print("[FLUX-INFINI] Flux arrêté.", flush=True)

@app.post("/commandes/flux-predictions/start")
def start_flux():
    """Démarre le flux infini de demandes de prédictions en arrière-plan."""
    global flux_simulation_running, flux_thread
    intervalle = float(request.args.get("intervalle", 1.0))
    if not flux_simulation_running:
        flux_simulation_running = True
        flux_thread = threading.Thread(target=boucle_flux_infini, args=(intervalle,), daemon=True)
        flux_thread.start()
        return jsonify(message="Flux infini démarré", intervalle=intervalle)
    return jsonify(message="Flux déjà en cours"), 200

@app.post("/commandes/flux-predictions/stop")
def stop_flux():
    """Arrête le flux infini de demandes de prédictions."""
    global flux_simulation_running
    flux_simulation_running = False
    return jsonify(message="Flux infini arrêté")

if __name__ == "__main__":
    # Consommateur des résultats de prédiction
    consumer_thread = threading.Thread(target=start_kafka_prediction_consumer, daemon=True)
    consumer_thread.start()

    app.run(host="0.0.0.0", port=8080)
