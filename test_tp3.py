import unittest
import json
import os
import sys

# Ajout des chemins
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MODEL_PATH"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prediction", "modele_commande.pkl")

from prediction.app import app as prediction_app, faire_prediction
from commandes.app import app as commandes_app

class TestTP3(unittest.TestCase):

    def test_prediction_direct_logic(self):
        sample_data = {
            "client_id": 25,
            "age": 32,
            "anciennete": 3,
            "nombre_commandes": 8,
            "montant_total": 1250,
            "montant_moyen": 156.25,
            "jours_depuis_derniere_commande": 12
        }
        pred, proba = faire_prediction(sample_data)
        self.assertIn(pred, [0, 1])
        self.assertTrue(0.0 <= proba <= 1.0)
        # Client actif -> prédiction 1
        self.assertEqual(pred, 1)
        self.assertGreater(proba, 0.70)

    def test_prediction_inactive_client(self):
        inactive_data = {
            "client_id": 99,
            "age": 60,
            "anciennete": 1,
            "nombre_commandes": 1,
            "montant_total": 25,
            "montant_moyen": 25,
            "jours_depuis_derniere_commande": 170
        }
        pred, proba = faire_prediction(inactive_data)
        self.assertIn(pred, [0, 1])
        self.assertTrue(0.0 <= proba <= 1.0)
        # Client inactif -> prédiction 0
        self.assertEqual(pred, 0)
        self.assertLess(proba, 0.40)

    def test_prediction_api_endpoints(self):
        client = prediction_app.test_client()

        # GET /health
        res_health = client.get("/health")
        self.assertEqual(res_health.status_code, 200)
        data_health = res_health.get_json()
        self.assertEqual(data_health["status"], "healthy")
        self.assertTrue(data_health["model_loaded"])

        # POST /predict
        payload = {
            "client_id": 25,
            "age": 32,
            "anciennete": 3,
            "nombre_commandes": 8,
            "montant_total": 1250,
            "montant_moyen": 156.25,
            "jours_depuis_derniere_commande": 12
        }
        res_predict = client.post("/predict", json=payload)
        self.assertEqual(res_predict.status_code, 200)
        data_predict = res_predict.get_json()
        self.assertEqual(data_predict["client_id"], 25)
        self.assertEqual(data_predict["prediction"], 1)
        self.assertGreater(data_predict["probabilite_commande"], 0.70)

    def test_commandes_api_endpoints(self):
        client = commandes_app.test_client()

        # GET /commandes
        res = client.get("/commandes")
        self.assertEqual(res.status_code, 200)
        cmds = res.get_json()
        self.assertGreaterEqual(len(cmds), 3)

        # GET /commandes/predictions
        res_pred = client.get("/commandes/predictions")
        self.assertEqual(res_pred.status_code, 200)
        self.assertIsInstance(res_pred.get_json(), dict)

if __name__ == "__main__":
    unittest.main()
