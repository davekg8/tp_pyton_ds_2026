from flask import Flask, jsonify, request
import json

app = Flask(__name__)

clients = [
	{"id": 1, "nom": "Alice", "pays": "France"},
	{"id": 2, "nom": "Alex", "pays": "Belgique"},
	{"id": 3, "nom": "Madeleine", "pays": "France"}
]

commandes = [
	{"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
	{"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
	{"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

@app.route("/")
def home():
	return "Hello ye"

@app.get("/clients")
def list_clients():
	return jsonify(clients)

@app.get("/clients/<int:id>")
def get_client(id):
	for i in clients:
		if (i["id"] == id):
			return jsonify(i)

	return jsonify(error = "Not Found"), 404

@app.get("/commandes")
def list_commandes():
	return jsonify(commandes)

@app.get("/commandes/<int:id>")
def get_commande(id):
	for i in commandes:
		if (i["id"] == id):
			# i["client"] = get_client(i["client_id"])
			
			return jsonify(i)

	return jsonify(error = "Not Found"), 404


if (__name__ == "__main__"):
	app.run(host = "0.0.0.0", port = 8080)
