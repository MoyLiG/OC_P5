// init-mongo.js
// Script d'initialisation MongoDB avec RBAC (Role-Based Access Control)
// À exécuter au démarrage du conteneur MongoDB

// Connexion en tant qu'admin root
db = db.getSiblingDB('admin');

print('=== Initialisation MongoDB RBAC ===');

// 1. Création de la base de données principale
db = db.getSiblingDB('P5');
print('✓ Base de données P5 créée');

// 2. Création des rôles personnalisés

// Rôle : Lecture seule sur toutes les collections sauf users
db.createRole({
    role: "lecteurMedical",
    privileges: [
        {
            resource: { db: "P5", collection: "dataset_donnees_medicales" },
            actions: ["find"]
        }
    ],
    roles: []
});
print('✓ Rôle lecteurMedical créé');

// Rôle : Écriture sur les données médicales
db.createRole({
    role: "redacteurMedical",
    privileges: [
        {
            resource: { db: "P5", collection: "dataset_donnees_medicales" },
            actions: ["find", "insert", "update"]
        }
    ],
    roles: []
});
print('✓ Rôle redacteurMedical créé');

// Rôle : Admin complet sur P5
db.createRole({
    role: "adminP5",
    privileges: [
        {
            resource: { db: "P5", collection: "" },
            actions: ["find", "insert", "update", "remove", "createCollection", "dropCollection", "createIndex", "dropIndex"]
        }
    ],
    roles: []
});
print('✓ Rôle adminP5 créé');

// 3. Création des utilisateurs MongoDB avec leurs rôles

// Utilisateur admin de la base P5
db.createUser({
    user: "admin_p5",
    pwd: "SecureAdminPassword123!",
    roles: [
        { role: "adminP5", db: "P5" }
    ]
});
print('✓ Utilisateur admin_p5 créé');

// Utilisateur lecture seule
db.createUser({
    user: "lecteur_p5",
    pwd: "SecureLecteurPassword123!",
    roles: [
        { role: "lecteurMedical", db: "P5" }
    ]
});
print('✓ Utilisateur lecteur_p5 créé');

// Utilisateur avec droits d'écriture
db.createUser({
    user: "redacteur_p5",
    pwd: "SecureRedacteurPassword123!",
    roles: [
        { role: "redacteurMedical", db: "P5" }
    ]
});
print('✓ Utilisateur redacteur_p5 créé');

// 4. Création de la base de test P5_test avec les mêmes rôles
db = db.getSiblingDB('P5_test');
print('✓ Base de données P5_test créée');

// Dupliquer les rôles pour P5_test
db.createRole({
    role: "lecteurMedical",
    privileges: [
        {
            resource: { db: "P5_test", collection: "dataset_donnees_medicales" },
            actions: ["find"]
        }
    ],
    roles: []
});

db.createRole({
    role: "redacteurMedical",
    privileges: [
        {
            resource: { db: "P5_test", collection: "dataset_donnees_medicales" },
            actions: ["find", "insert", "update"]
        }
    ],
    roles: []
});

db.createRole({
    role: "adminP5",
    privileges: [
        {
            resource: { db: "P5_test", collection: "" },
            actions: ["find", "insert", "update", "remove", "createCollection", "dropCollection", "createIndex", "dropIndex"]
        }
    ],
    roles: []
});

// Utilisateurs pour P5_test
db.createUser({
    user: "admin_p5_test",
    pwd: "TestAdminPassword123!",
    roles: [
        { role: "adminP5", db: "P5_test" }
    ]
});
print('✓ Utilisateur admin_p5_test créé');

print('=== Initialisation MongoDB RBAC terminée ===');

// 5. Affichage récapitulatif
db = db.getSiblingDB('admin');
print('\n📋 Récapitulatif des utilisateurs créés:');
print('Production (P5):');
print('  - admin_p5 (adminP5) : admin complet');
print('  - lecteur_p5 (lecteurMedical) : lecture seule');
print('  - redacteur_p5 (redacteurMedical) : lecture + écriture');
print('\nTest (P5_test):');
print('  - admin_p5_test (adminP5) : admin complet');
print('\n⚠️  IMPORTANT: Changez ces mots de passe en production !');
