# Commandes Terminal (Copier / Coller)

---

## 1. Démarrage de Docker & des Microservices

```bash
# Lancer tous les conteneurs (Kafka + Clients + Commandes + Statistiques + Prediction x3)
docker compose up --scale prediction=3 --build -d
```

```bash
# Vérifier l'état des conteneurs
docker compose ps
```

---

## 2. Démonstration TP1 (API REST Synchrone)

```bash
# 1. Lister les clients (service clients port 8080)
curl http://localhost:8080/clients
```

```bash
# 2. Récupérer la commande 1 (commandes appelle clients en REST)
curl http://localhost:8081/commandes/1
```

```bash
# 3. Tester le cas 404 (client inexistant)
curl http://localhost:8080/clients/999
```

---

## 3. Démonstration TP2 (Kafka Asynchrone & Statistiques)

```bash
# 1. Créer une commande (publie dans Kafka sur commandes-creees)
curl -X POST http://localhost:8081/commandes -H "Content-Type: application/json" -d "{\"client_id\": 2, \"produit\": \"PC\", \"montant\": 1200, \"date\": \"2026-10-12\"}"
```

```bash
# 2. Vérifier le compteur de commandes chez Clients
curl http://localhost:8080/clients/2/commandes-stats
```

```bash
# 3. Vérifier le CA et les commandes par produit chez Statistiques
curl http://localhost:8082/statistiques
```

```bash
# 4. Voir les logs Kafka en direct pour statistiques et clients
docker compose logs --tail=15 microservice-clients
docker compose logs --tail=15 microservice-statistiques
```

---

## 4. Démonstration TP3 (Machine Learning & Kafka)

```bash
# 1. Envoyer la demande de prédiction du sujet (Client 25)
curl -X POST http://localhost:8081/commandes/demander-prediction -H "Content-Type: application/json" -d "{\"client_id\": 25, \"age\": 32, \"anciennete\": 3, \"nombre_commandes\": 8, \"montant_total\": 1250, \"montant_moyen\": 156.25, \"jours_depuis_derniere_commande\": 12}"
```

```bash
# 2. Voir le log du microservice Prediction (calcul ML et probabilité)
docker compose logs --tail=15 prediction
```

```bash
# 3. Voir le log du microservice Commandes (résultat reçu)
docker compose logs --tail=15 microservice-commandes
```

```bash
# 4. Consulter le résultat stocké dans l'API Commandes
curl http://localhost:8081/commandes/predictions/25
```

---

## 5. TP3 Questions Avancées (Scaling K instances, Flux & Résilience)

```bash
# 1. Lancer 15 prédictions en flux continu (observer l'équilibrage entre instances)
python simulateur_flux.py --broker localhost:9092 --count 15 --interval 0.3
```

```bash
# 2. Vérifier que les 3 instances se partagent le travail
docker compose logs --tail=25 prediction
```

```bash
# 3. Test de panne : stopper brutalement une instance de prédiction
docker stop tp_pyton_ds_2026-prediction-1
```

```bash
# 4. Relancer un flux pour prouver que les instances restantes prennent le relais
python simulateur_flux.py --broker localhost:9092 --count 10 --interval 0.3
```

```bash
# 5. Redémarrer l'instance arrêtée
docker start tp_pyton_ds_2026-prediction-1
```

---

## 6. Tests Unitaires & Pipeline (Sans Docker)

```bash
# Exécuter les tests unitaires ML et API
python test_tp3.py
```

```bash
# Exécuter le test du pipeline complet
python test_pipeline.py
```

---

## 7. Arrêt des Conteneurs

```bash
# Arrêter tous les conteneurs
docker compose down
```
