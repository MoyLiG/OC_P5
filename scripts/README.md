# Migration de Données Médicales vers MongoDB avec Docker

## Description du Projet
Ce projet permet de transférer des données médicales depuis un fichier CSV vers une base de données MongoDB, en utilisant Docker pour conteneuriser l'environnement. Il inclut des scripts pour tester l'intégrité des données, optimiser la collection MongoDB, et formater les noms des patients.

---

## Prérequis
- Docker et Docker Compose installés sur votre machine.
- Un fichier CSV (`healthcare_dataset.csv`) contenant les données médicales.

---


## Installation et Configuration

### 1. Préparation de l'environnement
1. Téléchargez les fichiers du projet.
2. Placez votre fichier `healthcare_dataset.csv` dans le dossier `data/`.

### 2. Configuration de Docker
- Assurez-vous que Docker et Docker Compose sont installés.
- Ouvrez un terminal dans le dossier racine du projet.

### 3. Lancement des conteneurs
Exécutez la commande suivante pour démarrer les conteneurs :
```bash
docker-compose up
