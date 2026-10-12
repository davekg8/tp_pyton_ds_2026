#!/usr/bin/env python3
"""
Simulateur de flux infini de demandes de prédictions (TP3 - Section 3.7)

Ce script permet de générer un flux continu de demandes de prédiction
vers le topic Kafka 'demandes-predictions' (ou via l'API REST de commandes).
Il permet de tester :
  1. Le traitement en parallèle par K instances du microservice prediction.
  2. La montée en charge.
  3. La résilience à la panne (arrêt dynamique d'un ou deux conteneurs).

Usage:
  python simulateur_flux.py --mode kafka --broker localhost:9092 --interval 0.5
  python simulateur_flux.py --mode rest --url http://localhost:8081 --interval 0.5
"""

import argparse
import json
import random
import sys
import time

def generer_client_aleatoire(client_id):
    """Génère un profil client aléatoire et réaliste."""
    # Profils variés : clients très actifs, clients fidèles, nouveaux clients, dormants
    profil_type = random.choice(["tres_actif", "moyen", "nouveau", "dormant"])

    if profil_type == "tres_actif":
        age = random.randint(22, 55)
        anciennete = random.randint(2, 8)
        nb_commandes = random.randint(6, 25)
        panier = random.uniform(80, 220)
        jours = random.randint(1, 20)
    elif profil_type == "moyen":
        age = random.randint(25, 65)
        anciennete = random.randint(1, 5)
        nb_commandes = random.randint(2, 6)
        panier = random.uniform(40, 150)
        jours = random.randint(20, 60)
    elif profil_type == "nouveau":
        age = random.randint(18, 45)
        anciennete = 1
        nb_commandes = 1
        panier = random.uniform(30, 100)
        jours = random.randint(5, 30)
    else: # dormant
        age = random.randint(30, 70)
        anciennete = random.randint(3, 10)
        nb_commandes = random.randint(1, 3)
        panier = random.uniform(20, 80)
        jours = random.randint(90, 180)

    montant_total = round(nb_commandes * panier, 2)
    montant_moyen = round(panier, 2)

    return {
        "client_id": client_id,
        "age": age,
        "anciennete": anciennete,
        "nombre_commandes": nb_commandes,
        "montant_total": montant_total,
        "montant_moyen": montant_moyen,
        "jours_depuis_derniere_commande": jours,
        "profil_simule": profil_type
    }

def flux_via_kafka(broker, topic, interval, total_max=None):
    from kafka import KafkaProducer
    print(f"Connexion au broker Kafka {broker}...")
    producer = KafkaProducer(
        bootstrap_servers=broker.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"Démarrage du flux infini vers le topic '{topic}' (intervalle: {interval}s)")
    print("Appuyez sur Ctrl+C pour arrêter le flux.\n")

    client_id = 100
    compteur = 0
    try:
        while True:
            data = generer_client_aleatoire(client_id)
            producer.send(topic, value=data)
            producer.flush()
            compteur += 1
            print(
                f"[Flux #{compteur}] Client {data['client_id']} ({data['profil_simule']}) "
                f"-> Cmds:{data['nombre_commandes']} | Récence:{data['jours_depuis_derniere_commande']}j | "
                f"Total:{data['montant_total']}€"
            )
            client_id += 1
            if total_max and compteur >= total_max:
                print(f"Limite atteinte ({total_max} messages envoyés).")
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nFlux arrêté par l'utilisateur. Total envoyé : {compteur} événements.")

def flux_via_rest(url, interval, total_max=None):
    import requests
    endpoint = f"{url.rstrip('/')}/commandes/demander-prediction"
    print(f"Démarrage du flux infini vers l'API REST : {endpoint} (intervalle: {interval}s)")
    print("Appuyez sur Ctrl+C pour arrêter le flux.\n")

    client_id = 100
    compteur = 0
    try:
        while True:
            data = generer_client_aleatoire(client_id)
            try:
                resp = requests.post(endpoint, json=data, timeout=5)
                compteur += 1
                if resp.status_code == 202:
                    print(
                        f"[Flux REST #{compteur}] Client {data['client_id']} ({data['profil_simule']}) -> Envoyé avec succès (202)"
                    )
                else:
                    print(f"[Flux REST #{compteur}] Réponse status={resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"[Flux REST #{compteur}] Erreur HTTP : {e}")

            client_id += 1
            if total_max and compteur >= total_max:
                print(f"Limite atteinte ({total_max} requêtes).")
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nFlux arrêté. Total envoyé : {compteur} requêtes.")

def main():
    parser = argparse.ArgumentParser(description="Simulateur de flux infini de prédictions (TP3)")
    parser.add_argument("--mode", choices=["kafka", "rest"], default="kafka", help="Mode d'envoi (kafka direct ou via api rest)")
    parser.add_argument("--broker", default="localhost:9092", help="Broker Kafka (ex: localhost:9092 ou kafka:29092)")
    parser.add_argument("--topic", default="demandes-predictions", help="Topic de destination")
    parser.add_argument("--url", default="http://localhost:8081", help="URL du microservice commandes")
    parser.add_argument("--interval", type=float, default=0.5, help="Intervalle entre chaque message en secondes (défaut: 0.5s)")
    parser.add_argument("--count", type=int, default=None, help="Nombre total d'événements (par défaut infini)")

    args = parser.parse_args()

    if args.mode == "kafka":
        flux_via_kafka(args.broker, args.topic, args.interval, args.count)
    else:
        flux_via_rest(args.url, args.interval, args.count)

if __name__ == "__main__":
    main()
