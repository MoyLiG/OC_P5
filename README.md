# OC_P5 — Migration de Données Médicales vers MongoDB

Migration et optimisation d'un dataset de données médicales vers MongoDB, avec mise en place d'un système de contrôle d'accès basé sur les rôles (RBAC) et d'une pipeline de tests automatisés.

---

## Sommaire

- [Présentation du projet](#présentation-du-projet)
- [Architecture](#architecture)
- [Schéma NoSQL et choix de modélisation](#schéma-nosql-et-choix-de-modélisation)
- [Système RBAC](#système-rbac)
- [Prérequis](#prérequis)
- [Configuration](#configuration)
- [Déroulé du projet](#déroulé-du-projet)
  - [1. Tests en local](#1-tests-en-local)
  - [2. Déploiement en local](#2-déploiement-en-local)
  - [3. Tests sous Docker](#3-tests-sous-docker)
  - [4. Déploiement sous Docker](#4-déploiement-sous-docker)
- [Pipeline CI/CD](#pipeline-cicd)
- [Sécurité](#sécurité)

---

## Présentation du projet

Ce projet assure la migration d'un dataset de données médicales (format CSV) vers une base MongoDB, en appliquant :

- Un **nettoyage et une normalisation** des données (noms, dates)
- Une **optimisation** via des index adaptés aux requêtes métier
- Un **contrôle d'accès par rôles** (RBAC) pour sécuriser l'accès aux données sensibles
- Une **suite de tests** unitaires et d'intégration automatisés

---

## Architecture

```
OC_P5/
├── .github/
│   └── workflows/
│       └── run_tests.yml              # Pipeline CI/CD GitHub Actions
├── data/
│   └── healthcare_dataset.csv         # Dataset source
├── scripts/
│   ├── transfert_csv_mongodb.py       # Migration CSV → MongoDB
│   ├── optimiser_collection.py        # Normalisation + index
│   ├── test_integrite_donnees.py      # Vérification intégrité
│   ├── gestion_utilisateurs.py        # Gestion RBAC applicatif
│   └── test_rbac_integration.py       # Tests intégration RBAC
├── tests/
│   ├── test_transfert_csv_mongodb.py
│   ├── test_optimiser_collection.py
│   ├── test_test_integrite_donnees.py
│   └── test_gestion_utilisateurs.py
├── init-mongo.js                      # Initialisation RBAC MongoDB
├── docker-compose.yml                 # Déploiement production
├── docker-compose.test.yml            # Environnement de test Docker
├── mongod.conf                        # Configuration MongoDB
├── .env.example                       # Template des variables d'environnement
└── requirements.txt
```

---

## Schéma NoSQL et choix de modélisation

Chaque document de la collection `dataset_donnees_medicales` représente un séjour hospitalier :

```json
{
  "Name": "John Doe",
  "Age": 45,
  "Gender": "Male",
  "Blood Type": "A+",
  "Medical Condition": "Diabetes",
  "Date of Admission": "2024-01-15T00:00:00Z",
  "Discharge Date": "2024-01-22T00:00:00Z",
  "Doctor": "Dr. Smith",
  "Hospital": "General Hospital",
  "Insurance Provider": "Aetna",
  "Billing Amount": 15230.50,
  "Room Number": 302,
  "Admission Type": "Elective",
  "Medication": "Metformin",
  "Test Results": "Normal"
}
```

### Optimisations appliquées

| Index | Champ(s) | Justification |
|---|---|---|
| `Name_1` | `Name` | Recherche par patient |
| `Medical_Condition_1` | `Medical Condition` | Filtrage par pathologie |
| `Date_of_Admission_1` | `Date of Admission` | Tri et filtrage temporel |
| `Medical_Condition_1_Age_1` | `Medical Condition` + `Age` | Requêtes analytiques combinées |
| `text_index` | Tous les champs | Recherche textuelle libre |

Les dates (`Date of Admission`, `Discharge Date`) sont converties du format `string` au type `Date` natif MongoDB pour permettre des requêtes temporelles efficaces.

---

## Système RBAC

### Rôles MongoDB (niveau base de données)

| Rôle | Permissions |
|---|---|
| `adminP5` | CRUD complet + gestion des collections et index |
| `redacteurMedical` | Lecture + insertion + mise à jour |
| `lecteurMedical` | Lecture seule |

### Rôles applicatifs (niveau application)

| Rôle | Permissions |
|---|---|
| `admin` | Tous les droits + gestion des utilisateurs |
| `medecin` | Lecture + écriture des dossiers médicaux |
| `infirmier` | Lecture + écriture limitée |
| `lecteur` | Consultation uniquement |

Les mots de passe applicatifs sont hashés avec **bcrypt** (12 rounds).

---

## Prérequis

- Python 3.11+
- Docker & Docker Compose
- Java 11+ (si utilisation de BFG pour nettoyage Git)

```bash
pip install -r requirements.txt
```

---

## Configuration

Copie le fichier exemple et renseigne tes valeurs :

```bash
cp .env.example .env.local
```

Variables requises :

```dotenv
MONGO_ROOT_USER=root
MONGO_ROOT_PASSWORD=        # À générer
MONGO_ADMIN_PASSWORD=       # À générer
MONGO_LECTEUR_PASSWORD=     # À générer
MONGO_REDACTEUR_PASSWORD=   # À générer
MONGO_ADMIN_TEST_PASSWORD=  # À générer
MONGO_URI=mongodb://...
DB_NAME=P5
CSV_FILE_PATH=./data/healthcare_dataset.csv
```

Générer un mot de passe fort :
```bash
python -c "import secrets, string; print(''.join(secrets.choice(string.ascii_letters + string.digits + '!@#$%') for _ in range(20)))"
```

---

## Déroulé du projet

### 1. Tests en local

Exécution des tests unitaires directement sur la machine, sans Docker :

```bash
# Depuis la racine du projet
pytest tests/test_transfert_csv_mongodb.py -v
pytest tests/test_optimiser_collection.py -v
pytest tests/test_test_integrite_donnees.py -v
pytest tests/test_gestion_utilisateurs.py -v
```

Les tests unitaires utilisent des mocks — aucune connexion MongoDB n'est requise.

---

### 2. Déploiement en local

Exécution des scripts de migration directement sur la machine avec une instance MongoDB locale :

```bash
# Configurer l'environnement
cp .env.example .env.local
# Renseigner les variables dans .env.local

# Lancer les scripts
python scripts/transfert_csv_mongodb.py
python scripts/optimiser_collection.py
python scripts/test_integrite_donnees.py
```

---

### 3. Tests sous Docker

L'environnement Docker de test (`docker-compose.test.yml`) reproduit fidèlement le workflow GitHub Actions : un service `mongodb_test` et un service `test_runner` qui exécute l'ensemble des tests unitaires et d'intégration.

```bash
# Configurer l'environnement
cp .env.example .env.test_docker
# Renseigner les variables dans .env.test_docker

# Lancer l'environnement de test
docker compose -f docker-compose.test.yml up --build

# Consulter les logs
docker logs test_runner
```

Ce setup permet de valider localement que le pipeline CI/CD fonctionnera avant tout push sur `main`.

---

### 4. Déploiement sous Docker

Déploiement complet de la stack en production :

```bash
# Configurer l'environnement
cp .env.example .env.docker
# Renseigner les variables dans .env.docker

# Lancer la stack
docker compose up --build
```

Le service `migration` exécute automatiquement la migration et l'optimisation une fois MongoDB disponible.

---

## Pipeline CI/CD

Le workflow GitHub Actions (`.github/workflows/run_tests.yml`) se déclenche à chaque push ou pull request sur `main` :

```
1. Démarrage du service MongoDB
2. Initialisation RBAC (init-mongo.js)
3. Tests unitaires (mocks, sans MongoDB)
4. Tests d'intégration RBAC
5. Tests d'intégration migration des données
```

### GitHub Secrets requis

| Secret | Description |
|---|---|
| `MONGO_ROOT_PASSWORD` | Mot de passe root MongoDB |
| `MONGO_ADMIN_PASSWORD` | Mot de passe admin_p5 |
| `MONGO_LECTEUR_PASSWORD` | Mot de passe lecteur_p5 |
| `MONGO_REDACTEUR_PASSWORD` | Mot de passe redacteur_p5 |
| `MONGO_ADMIN_TEST_PASSWORD` | Mot de passe admin_p5_test |

---

## Sécurité

- Les secrets ne sont **jamais commités** — tous les fichiers `.env.*` sont dans `.gitignore`
- Les mots de passe MongoDB sont injectés via **variables d'environnement** dans `init-mongo.js`
- Les mots de passe applicatifs sont hashés avec **bcrypt** (résistant aux attaques GPU)
- Le principe du **moindre privilège** est appliqué : chaque utilisateur n'a accès qu'aux ressources nécessaires à son rôle
- Les secrets CI/CD sont gérés via **GitHub Secrets**
