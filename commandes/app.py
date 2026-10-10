import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# URL du microservice clients (configurable par variable d'environnement pour Docker)
CLIENTS_SERVICE_URL = os.environ.get("CLIENTS_SERVICE_URL", "http://localhost:8080")

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

@app.route("/")
def home():
    return "Microservice Commandes"

@app.get("/commandes")
def list_commandes():
    return jsonify(commandes)

@app.get("/commandes/<int:id>")
def get_commande(id):
    # 1. Recherche de la commande
    commande_trouvee = None
    for commande in commandes:
        if commande["id"] == id:
            commande_trouvee = commande
            break

    # Si la commande n'existe pas -> 404
    if not commande_trouvee:
        return jsonify(error="Commande Not Found"), 404

    # 2. Appel au microservice clients
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
        # Gestion du cas où le microservice clients n'est pas disponible
        return jsonify(
            error="Le microservice clients n'est pas disponible",
            commande=commande_trouvee
        ), 503

    # 3. Retour de la commande enrichie
    resultat = {
        "id": commande_trouvee["id"],
        "produit": commande_trouvee["produit"],
        "montant": commande_trouvee["montant"],
        "client": client_data
    }
    return jsonify(resultat)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
