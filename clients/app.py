from flask import Flask, jsonify

app = Flask(__name__)

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

@app.route("/")
def home():
    return "Microservice Clients"

@app.get("/clients")
def list_clients():
    return jsonify(clients)

@app.get("/clients/<int:id>")
def get_client(id):
    for client in clients:
        if client["id"] == id:
            return jsonify(client)
    return jsonify(error="Not Found"), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
