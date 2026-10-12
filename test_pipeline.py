"""
Test d'intégration end-to-end simulant la chaîne asynchrone complète TP3 :
commandes -> (demandes-predictions) -> prediction -> (resultats-predictions) -> commandes
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["MODEL_PATH"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prediction", "modele_commande.pkl")

from prediction.app import faire_prediction, model
from commandes.app import resultats_predictions, resultats_lock

def test_full_pipeline():
    print("=== Test du Pipeline Asynchrone TP3 ===")

    # 1. Événement produit par commandes
    demande = {
        "client_id": 25,
        "age": 32,
        "anciennete": 3,
        "nombre_commandes": 8,
        "montant_total": 1250,
        "montant_moyen": 156.25,
        "jours_depuis_derniere_commande": 12
    }
    print("1. [COMMANDES] Préparation de la demande pour le topic 'demandes-predictions':")
    print("  ", demande)

    # 2. Traitement par le microservice prediction
    prediction, probabilite = faire_prediction(demande)
    print("\n2. [PREDICTION] Consommation et inférence ML :")
    print(f"   Client : {demande['client_id']}")
    print(f"   Prediction : {prediction}")
    print(f"   Probabilite : {round(probabilite, 4)}")

    resultat_event = {
        "client_id": demande["client_id"],
        "prediction": int(prediction),
        "probabilite_commande": round(float(probabilite), 4)
    }
    print("\n3. [PREDICTION] Événement publié sur 'resultats-predictions':")
    print("  ", resultat_event)

    # 3. Réception par le microservice commandes
    with resultats_lock:
        resultats_predictions[demande["client_id"]] = resultat_event

    print("\n4. [COMMANDES] Réception et affichage du résultat :")
    print(f"   Client ID : {resultats_predictions[25]['client_id']}")
    print(f"   Prédiction : {resultats_predictions[25]['prediction']} (Passera commande)")
    print(f"   Probabilité : {resultats_predictions[25]['probabilite_commande'] * 100:.2f}%")

    assert resultats_predictions[25]["prediction"] == 1
    assert resultats_predictions[25]["probabilite_commande"] > 0.70
    print("\n Pipeline de bout en bout validé avec succès !")

if __name__ == "__main__":
    test_full_pipeline()
