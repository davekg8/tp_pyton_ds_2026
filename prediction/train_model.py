import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score
from sklearn.model_selection import train_test_split

def generer_donnees_clients(n_samples=3000, random_state=42):
    """
    Génère un jeu de données synthétique équilibré basé sur les caractéristiques
    clients décrites dans le TP3 (analyse RFM : Récence, Fréquence, Montant).
    """
    np.random.seed(random_state)

    age = np.random.randint(18, 70, size=n_samples)
    anciennete = np.random.randint(1, 12, size=n_samples)
    nombre_commandes = np.random.geometric(p=0.15, size=n_samples) # 1 à ~25 commandes
    nombre_commandes = np.clip(nombre_commandes, 0, 30)

    # Panier moyen
    panier_moyen = np.random.normal(loc=110, scale=40, size=n_samples)
    panier_moyen = np.clip(np.round(panier_moyen, 2), 20, 350)
    montant_total = np.round(nombre_commandes * panier_moyen, 2)

    # Jours depuis dernière commande
    jours_depuis_derniere_commande = np.random.randint(1, 180, size=n_samples)

    # Facteur récence normalisé (1 = hier, 0 = 180 jours)
    f_recence = (180.0 - jours_depuis_derniere_commande) / 180.0
    # Facteur fréquence normalisé
    f_frequence = np.clip(nombre_commandes / 12.0, 0.0, 1.5)
    # Facteur fidélité (ancienneté)
    f_anciennete = anciennete / 10.0

    # Score logistique centré pour avoir un ratio équilibré ~45-55%
    z = (
        -1.35
        + 2.1 * f_recence
        + 1.3 * f_frequence
        + 0.5 * f_anciennete
        + np.random.normal(0, 0.4, size=n_samples)
    )
    proba = 1.0 / (1.0 + np.exp(-z))
    cible = (proba >= 0.5).astype(int)

    df = pd.DataFrame({
        "age": age,
        "anciennete": anciennete,
        "nombre_commandes": nombre_commandes,
        "montant_total": montant_total,
        "montant_moyen": panier_moyen,
        "jours_depuis_derniere_commande": jours_depuis_derniere_commande,
        "passera_commande": cible
    })

    return df

def entrainer_et_sauvegarder_modele(chemin_modele="modele_commande.pkl"):
    print("=== Entraînement du modèle de Machine Learning pour TP3 ===")
    df = generer_donnees_clients()

    print(f"Distribution des classes :\n{df['passera_commande'].value_counts(normalize=True)}")

    features = [
        "age",
        "anciennete",
        "nombre_commandes",
        "montant_total",
        "montant_moyen",
        "jours_depuis_derniere_commande"
    ]
    X = df[features]
    y = df["passera_commande"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
        class_weight="balanced"
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    roc = roc_auc_score(y_test, y_proba)
    print(f"\nAccuracy sur test : {acc:.4f}")
    print(f"ROC AUC sur test  : {roc:.4f}")
    print("\nRapport de classification :")
    print(classification_report(y_test, y_pred, zero_division=0))

    # Sauvegarde du modèle avec joblib
    joblib.dump(model, chemin_modele)
    print(f"Modèle sauvegardé avec succès dans : {chemin_modele}")

    # Test avec l'exemple du sujet TP3
    exemple_sujet = pd.DataFrame([{
        "age": 32,
        "anciennete": 3,
        "nombre_commandes": 8,
        "montant_total": 1250.0,
        "montant_moyen": 156.25,
        "jours_depuis_derniere_commande": 12
    }])
    pred_test = int(model.predict(exemple_sujet)[0])
    proba_test = float(model.predict_proba(exemple_sujet)[0][1])
    print("\n--- Test avec l'exemple du sujet TP3 ---")
    print(f"Input : {exemple_sujet.to_dict(orient='records')[0]}")
    print(f"Prédiction : {pred_test} (1 = passera commande, 0 = ne passera pas)")
    print(f"Probabilité de commande : {proba_test:.4f} ({proba_test*100:.2f}%)")

    # Test avec un client inactif (ne devrait pas passer commande)
    exemple_inactif = pd.DataFrame([{
        "age": 55,
        "anciennete": 1,
        "nombre_commandes": 1,
        "montant_total": 35.0,
        "montant_moyen": 35.0,
        "jours_depuis_derniere_commande": 160
    }])
    pred_inactif = int(model.predict(exemple_inactif)[0])
    proba_inactif = float(model.predict_proba(exemple_inactif)[0][1])
    print("\n--- Test avec client inactif ---")
    print(f"Input : {exemple_inactif.to_dict(orient='records')[0]}")
    print(f"Prédiction : {pred_inactif}")
    print(f"Probabilité de commande : {proba_inactif:.4f} ({proba_inactif*100:.2f}%)")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    model_output_path = os.path.join(current_dir, "modele_commande.pkl")
    entrainer_et_sauvegarder_modele(model_output_path)
