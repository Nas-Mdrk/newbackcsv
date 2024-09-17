import os
import gc
import dask.dataframe as dd
from models import FileNameGenerator
from celery import current_app as celery
from models import User, FileNameGenerator
import re
import pandas as pd
import json
from datetime import datetime,date
import requests
import validators
from models import TableauDeBord
from extensions import db
import logging
import shutil

# Configurer le logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#celery = make_celery(None)  # Celery instance will be initialized in app.py

@celery.task
def add(x, y):
    return x + y

@celery.task
def delete_folder_contents(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

# from celery.schedules import crontab

# celery.conf.beat_schedule = {
#     'delete-at-17-00-everyday': {
#         'task': 'celery_tasks.delete_folder_contents',
#         'schedule': crontab(hour=11, minute=45),  # Tous les jours à 17h
#         'args': ('/app/uploads',)
#     },
# }

@celery.task
def clean_csv(file_path):
    """Nettoie un fichier CSV en supprimant les doublons et sauvegarde le fichier nettoyé"""
    # Lire le fichier CSV
    df = dd.read_csv(file_path,dtype='object')
    # Supprimer les doublons
    df_cleaned = df.drop_duplicates()

    # Générer un nom unique pour le fichier nettoyé
    cleaned_file_path = FileNameGenerator.generate_unique_filename(file_path)
    # Sauvegarder le fichier nettoyé dans un fichier temporaire
    df_cleaned.to_csv(cleaned_file_path,single_file=True, index=False)

    # Supprimer le fichier original
    if os.path.exists(file_path):
        os.remove(file_path)

    # Retourner le chemin du fichier nettoyé
    return cleaned_file_path


@celery.task(bind=True)
def remove_duplicates_key(self, file_path, key_column):#, output_file_path):
    try:
        """Nettoie un fichier CSV en supprimant les doublons et sauvegarde le fichier nettoyé"""
        # Lire le fichier CSV avec Dask
        df = dd.read_csv(file_path,dtype='object')
        
        # Vérifier si la colonne clé existe dans le DataFrame
        if key_column not in df.columns:
            raise ValueError(f"La colonne clé '{key_column}' n'existe pas dans le fichier.")

        # Supprimer les doublons en gardant la première occurrence
        df_cleaned = df.drop_duplicates(subset=key_column, keep='first')

        # Générer un nom unique pour le fichier nettoyé
        cleaned_file_path = FileNameGenerator.generate_unique_filename(file_path)#output_file_path)

        # Sauvegarder le DataFrame nettoyé dans un fichier temporaire
        df_cleaned.to_csv(cleaned_file_path, single_file=True, index=False)

        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)

        # Retourner le chemin du fichier nettoyé
        return cleaned_file_path
    except Exception as e:
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e

@celery.task(bind=True)
def clean_two_csvnew(self, file_path1, file_path2):
    try:
        # Lire les deux fichiers CSV en optimisant la mémoire et la vitesse
        df1 = dd.read_csv(file_path1, encoding='ISO-8859-1')
        df2 = dd.read_csv(file_path2, encoding='ISO-8859-1')

        # Utiliser Dask pour trouver les lignes communes
        common_rows = dd.merge(df1, df2, how='inner', on=list(df1.columns))

        # Filtrage des lignes en utilisant Dask
        df2_filtered = df2.map_partitions(
            lambda part: part.loc[~part.isin(common_rows.compute()).any(axis=1)]
        )

        # Nettoyage des doublons
        df1_cleaned = df1.drop_duplicates()
        df2_cleaned = df2_filtered.drop_duplicates()

        # Générer des noms uniques pour les fichiers nettoyés
        cleaned_file_path1 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path1))
        cleaned_file_path2 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path2))
        
        save_directory = '/app/uploads'
        cleaned_file_path1 = os.path.join(save_directory, cleaned_file_path1)
        cleaned_file_path2 = os.path.join(save_directory, cleaned_file_path2)

        # Sauvegarder les fichiers nettoyés
        df1_cleaned.to_csv(cleaned_file_path1, single_file=True, index=False)
        df2_cleaned.to_csv(cleaned_file_path2, single_file=True, index=False)
        
        # Supprimer les fichiers originaux
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        
        return cleaned_file_path1, cleaned_file_path2

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée à cause de: {e}")
        raise ValueError(f"Tâche échouée à cause de: {e}")

@celery.task(bind=True)
def clean_two_csv(self, file_path1, file_path2):
    try:
        # Lire les deux fichiers CSV
        df1 = dd.read_csv(file_path1, encoding='ISO-8859-1',dtype='object')
        df2 = dd.read_csv(file_path2, encoding='ISO-8859-1',dtype='object')

        # Convertir les DataFrames Dask en DataFrames pandas pour comparaison
        df1_pd = df1.compute()
        df2_pd = df2.compute()
        
        # Identifier les lignes communes
        common_rows = pd.merge(df1_pd, df2_pd, how='inner', on=list(df1_pd.columns))

        # Convertir common_rows en DataFrame pandas pour obtenir les lignes communes
        common_rows_set = set(tuple(row) for row in common_rows.to_records(index=False))

        # Fonction pour filtrer les lignes
        def filter_func(df, common_rows_set):
            return df[~df.apply(lambda row: tuple(row) in common_rows_set, axis=1)]

        # Appliquer la fonction de filtrage
        df2_filtered = filter_func(df2_pd, common_rows_set)

        # Nettoyage des doublons (optionnel)
        df1_cleaned = df1_pd.drop_duplicates()
        df2_cleaned = df2_filtered.drop_duplicates()

        # Générer des noms uniques pour les fichiers nettoyés
        cleaned_file_path1 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path1))
        cleaned_file_path2 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path2))
        
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path1 = os.path.join(save_directory, cleaned_file_path1)
        cleaned_file_path2 = os.path.join(save_directory, cleaned_file_path2)

        # Sauvegarder les fichiers nettoyés
        df1_cleaned.to_csv(cleaned_file_path1, index=False)
        df2_cleaned.to_csv(cleaned_file_path2, index=False)
        
        # Supprimer les fichiers originaux
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        
        return cleaned_file_path1, cleaned_file_path2
    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée à cause de: {e}")
        self.update_state(
            state='FAILURE',
            meta={'exc_type': type(e).__name__, 'exc_message': str(e)}
        )
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e
   
@celery.task(bind=True)
def Find_A_and_B_middle(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV en utilisant des chunks optimisés
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')

        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()

        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")

        # Vérifier les doublons dans la colonne commune
        unique_a = df_a[common_column].drop_duplicates().compute()
        if len(unique_a) < len(df_a):
            raise ValueError(f"Des doublons ont été trouvés dans la colonne commune '{common_column}' dans le fichier A.")

        unique_b = df_b[common_column].drop_duplicates().compute()
        if len(unique_b) < len(df_b):
            raise ValueError(f"Des doublons ont été trouvés dans la colonne commune '{common_column}' dans le fichier B.")

        # Renommer les colonnes similaires dans df_b pour éviter les conflits
        rename_columns = {col: col + '_bis' for col in df_b.columns if col in df_a.columns and col != common_column}
        df_b = df_b.rename(columns=rename_columns)

        # Fusionner les deux DataFrames sur la colonne commune avec une jointure interne
        df_merged = dd.merge(df_a, df_b, on=common_column, how='inner')

        # Réorganiser les colonnes pour inclure toutes les colonnes
        columns_a = list(df_a.columns)
        columns_b = [rename_columns.get(col, col) for col in df_b.columns if col != common_column]
        df_merged = df_merged[columns_a + columns_b]

        # Générer un nom unique pour le fichier fusionné
        merged_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_a))
        save_directory = '/app/uploads'
        merged_file_path = os.path.join(save_directory, merged_file_path)

        # Sauvegarder le DataFrame fusionné dans un fichier
        df_merged.to_csv(merged_file_path, single_file=True, index=False)

        # Supprimer les fichiers originaux
        for file in [file_a, file_b]:
            if os.path.exists(file):
                os.remove(file)

        # Retourner le chemin du fichier fusionné
        return merged_file_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        for file in [file_a, file_b]:
            if os.path.exists(file):
                os.remove(file)
        # Log de l'erreur
        print(f"Tâche échouée: {e}")
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e


@celery.task(bind=True) #uniquement dans a left b.key null 
def find_A_throw_B(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV avec des tailles de chunk optimisées
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')
        
        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()
        
        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")
        
        # Trouver les doublons en comptant les occurrences dans df_a
        counts_a = df_a.groupby(common_column).size()
        duplicates_a = counts_a[counts_a > 1]

        if not duplicates_a.compute().empty:
            duplicate_values_a = duplicates_a.compute().to_dict()
            raise ValueError(f"Des valeurs dupliquées ont été trouvées dans la colonne commune '{common_column}' dans le fichier A : {duplicate_values_a}")
            
        # Vérification des doublons dans le fichier B
        counts_b = df_b.groupby(common_column).size()
        duplicates_b = counts_b[counts_b > 1]

        if not duplicates_b.compute().empty:
            duplicate_values_b = duplicates_b.compute().to_dict()
            raise ValueError(f"Des valeurs dupliquées ont été trouvées dans la colonne commune '{common_column}' dans le fichier B : {duplicate_values_b}")
        
        # Renommer les colonnes de df_b pour éviter les conflits, sauf pour la colonne commune
        rename_columns = {col: col + '_bis' for col in df_b.columns if col != common_column and col in df_a.columns}
        df_b = df_b.rename(columns=rename_columns)

        # Fusionner les deux DataFrames sur la colonne commune spécifiée avec une jointure gauche
        df_merged = dd.merge(df_a, df_b, on=common_column, how='left', indicator=True)
        
        # Filtrer pour trouver les lignes où il n'y a pas de correspondance dans df_b
        df_non_matching = df_merged[df_merged['_merge'] == 'left_only']
        
        # Supprimer la colonne d'indicateur
        df_non_matching = df_non_matching.drop(columns=['_merge'])

        # Garder uniquement les colonnes de fichier1
        df_non_matching = df_non_matching.loc[:, df_a.columns]
        
        # Trier le DataFrame selon la colonne commune
        df_non_matching = df_non_matching.sort_values(by=common_column)
        
        # Générer un nom de fichier unique pour les lignes non correspondantes
        df_non_matching_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_a))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        df_non_matching_path = os.path.join(save_directory, df_non_matching_path)
        
        # Calculer le DataFrame Dask pour obtenir un DataFrame Pandas et sauvegarder le résultat
        df_non_matching = df_non_matching.compute()
        df_non_matching.to_csv(df_non_matching_path, index=False)
        
        # Supprimer les fichiers originaux
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        
        return df_non_matching_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e

def find_A_throw_B_base(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV avec des tailles de chunk optimisées
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')
        
        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()
        
        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")
        
        # Trouver les doublons en comptant les occurrences dans df_a
        counts_a = df_a.groupby(common_column).size()
        duplicates_a = counts_a[counts_a > 1]

        if not duplicates_a.compute().empty:
            duplicate_values_a = duplicates_a.compute().to_dict()
            raise ValueError(f"Des valeurs dupliquées ont été trouvées dans la colonne commune '{common_column}' dans le fichier A : {duplicate_values_a}")
            
        # Vérification des doublons dans le fichier B
        counts_b = df_b.groupby(common_column).size()
        duplicates_b = counts_b[counts_b > 1]

        if not duplicates_b.compute().empty:
            duplicate_values_b = duplicates_b.compute().to_dict()
            raise ValueError(f"Des valeurs dupliquées ont été trouvées dans la colonne commune '{common_column}' dans le fichier B : {duplicate_values_b}")
        
        # Renommer les colonnes de df_b pour éviter les conflits, sauf pour la colonne commune
        rename_columns = {col: col + '_bis' for col in df_b.columns if col != common_column and col in df_a.columns}
        df_b = df_b.rename(columns=rename_columns)

        # Fusionner les deux DataFrames sur la colonne commune spécifiée avec une jointure gauche
        df_merged = dd.merge(df_a, df_b, on=common_column, how='left', indicator=True)
        
        # Filtrer pour trouver les lignes où il n'y a pas de correspondance dans df_b
        df_non_matching = df_merged[df_merged['_merge'] == 'left_only']
        
        # Supprimer la colonne d'indicateur
        df_non_matching = df_non_matching.drop(columns=['_merge'])

        # Garder uniquement les colonnes de fichier1
        df_non_matching = df_non_matching.loc[:, df_a.columns]
        
        # Générer un nom de fichier unique pour les lignes non correspondantes
        df_non_matching_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_a))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        df_non_matching_path = os.path.join(save_directory, df_non_matching_path)
        
        # Calculer le DataFrame Dask pour obtenir un DataFrame Pandas et sauvegarder le résultat
        df_non_matching = df_non_matching.compute()
        df_non_matching.to_csv(df_non_matching_path, index=False)
        
        # Supprimer les fichiers originaux
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        
        return df_non_matching_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e
   
@celery.task(bind=True) #dans A avec B null si colonne vide left
def find_A_fill_B(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV en utilisant des chunks optimisés
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')
        
        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()
        
        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")
        
        # Ajouter une colonne d'index pour conserver l'ordre initial de df_a
        df_a = df_a.reset_index()
        df_a = df_a.rename(columns={'index': 'original_index'})
        
        # Trouver les doublons dans df_a
        counts_a = df_a.groupby(common_column).size()
        duplicates_a = counts_a[counts_a > 1].compute()
        if len(duplicates_a) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier A : {duplicates_a.to_dict()}")
                
        # Vérifier les doublons dans df_b
        counts_b = df_b.groupby(common_column).size()
        duplicates_b = counts_b[counts_b > 1].compute()
        if len(duplicates_b) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier B : {duplicates_b.to_dict()}")
        
        # Déterminer les colonnes non conflictuelles dans df_b
        non_conflicting_columns_b = [col for col in df_b.columns if col == common_column or col not in df_a.columns]
        
        # Fusionner les deux DataFrames sur la colonne commune spécifiée avec une jointure gauche
        df_merged = dd.merge(df_a, df_b[non_conflicting_columns_b], on=common_column, how='left')
        
        # Remplacer les valeurs manquantes par 'null' pour les colonnes ajoutées
        df_merged = df_merged.fillna('null')
        
        # Trier le DataFrame fusionné selon l'ordre d'origine du fichier A
        df_merged = df_merged.sort_values(by='original_index')
        
        # Supprimer la colonne d'index ajoutée
        df_merged = df_merged.drop(columns='original_index')
        
        # Générer un nom de fichier unique pour les lignes fusionnées
        df_merged_path = f"/app/uploads/{os.path.basename(file_a)}_merged.csv"
        
        # Utiliser compute avec Dask pour s'assurer que les résultats sont correctement sauvegardés
        df_merged.compute().to_csv(df_merged_path, index=False)

        # Supprimer les fichiers originaux
        os.remove(file_a)
        os.remove(file_b)

        return df_merged_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        print(f"Tâche échouée: {e}")
        raise e


def find_A_fill_B_base(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV en utilisant des chunks optimisés
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')
        
        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()
        
        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")
        
        # Trouver les doublons dans df_a
        counts_a = df_a.groupby(common_column).size()
        duplicates_a = counts_a[counts_a > 1]
        if len(duplicates_a.compute()) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier A : {duplicates_a.compute().to_dict()}")
                
        # Vérifier les doublons dans df_b
        counts_b = df_b.groupby(common_column).size()
        duplicates_b = counts_b[counts_b > 1]
        if len(duplicates_b.compute()) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier B : {duplicates_b.compute().to_dict()}")
        
        # Déterminer les colonnes non conflictuelles dans df_b
        non_conflicting_columns_b = [col for col in df_b.columns if col == common_column or col not in df_a.columns]
        
        # Fusionner les deux DataFrames sur la colonne commune spécifiée avec une jointure gauche
        df_merged = dd.merge(df_a, df_b[non_conflicting_columns_b], on=common_column, how='left')
        
        # Remplacer les valeurs manquantes par 'null' pour les colonnes ajoutées
        df_merged = df_merged.fillna('null')
        
        # Générer un nom de fichier unique pour les lignes fusionnées
        df_merged_path = f"/app/uploads/{os.path.basename(file_a)}_merged.csv"
        df_merged.compute().to_csv(df_merged_path, index=False)

        # Supprimer les fichiers originaux
        os.remove(file_a)
        os.remove(file_b)

        return df_merged_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        print(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def full_outer_join(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV en utilisant des chunks optimisés
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')

        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()

        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")

        # Trouver les doublons en utilisant groupby avec size
        counts_a = df_a.groupby(common_column).size()
        duplicates_a = counts_a[counts_a > 1]
        if len(duplicates_a.compute()) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier A : {duplicates_a.compute().to_dict()}")

        counts_b = df_b.groupby(common_column).size()
        duplicates_b = counts_b[counts_b > 1]
        if len(duplicates_b.compute()) > 0:
            raise ValueError(f"Des doublons ont été trouvés dans le fichier B : {duplicates_b.compute().to_dict()}")

        # Déterminer les colonnes non conflictuelles dans df_b
        non_conflicting_columns_b = [col for col in df_b.columns if col == common_column or col not in df_a.columns]

        # Fusionner les DataFrames avec une jointure extérieure complète
        df_merged = dd.merge(df_a, df_b[non_conflicting_columns_b], on=common_column, how='outer')

        # Remplacer les valeurs manquantes par 'null'
        df_merged = df_merged.fillna('null')

        # Générer un nom unique pour le fichier fusionné
        merged_file_path = f"/app/uploads/{os.path.basename(file_a)}_full_outer_join.csv"
        df_merged.compute().to_csv(merged_file_path, index=False)

        # Supprimer les fichiers originaux
        os.remove(file_a)
        os.remove(file_b)

        return merged_file_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        print(f"Tâche échouée: {e}")
        raise e

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# @celery.task(bind=True)
# def full_outer_join_with_null(self, file_a, file_b, common_column):
#     try:
#         # Lire les fichiers CSV en utilisant des chunks optimisés
#         df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
#         df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')

#         # Nettoyer les noms de colonnes
#         df_a.columns = df_a.columns.str.strip()
#         df_b.columns = df_b.columns.str.strip()

#         # Vérifier si la colonne commune existe dans les deux fichiers
#         if common_column not in df_a.columns or common_column not in df_b.columns:
#             raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")

#         # Ajouter une colonne d'index globale à chaque DataFrame
#         df_a = df_a.reset_index().rename(columns={'index': 'global_index'})
#         df_b = df_b.reset_index().rename(columns={'index': 'global_index'})

#         # Fusionner les DataFrames avec une jointure extérieure complète sans suffixes
#         df_merged = dd.merge(df_a, df_b, on=common_column, how='outer', suffixes=('', '_b'))

#         # Log des données avant filtrage
#         logging.info("Aperçu des données avant filtrage:")
#         preview_before = df_merged.head(10)  # Obtenir un aperçu des 10 premières lignes
#         logging.info(preview_before)

#         # Filtrer les lignes où la clé est null dans l'une ou l'autre des tables
#         df_filtered = df_merged[
#             df_merged[common_column].isna() |
#             df_merged[[col for col in df_merged.columns if col in df_a.columns and col != common_column]].isna().any(axis=1) |
#             df_merged[[col for col in df_merged.columns if col in df_b.columns and col != common_column]].isna().any(axis=1)
#         ]

#         # Log des données après filtrage
#         logging.info("Aperçu des données après filtrage:")
#         preview_after = df_filtered.head(10)  # Obtenir un aperçu des 10 premières lignes
#         logging.info(preview_after)

#         # Remplacer les valeurs manquantes par 'null'
#         df_filtered = df_filtered.fillna('null')

#         # Supprimer la colonne d'index global avant la sauvegarde
#         df_filtered = df_filtered.drop(columns=['global_index'], errors='ignore')

#         # Générer un nom unique pour le fichier filtré
#         filtered_file_path = f"/app/uploads/{os.path.basename(file_a)}_filtered.csv"
#         df_filtered.compute().to_csv(filtered_file_path, index=False)

#         # Supprimer les fichiers originaux
#         os.remove(file_a)
#         os.remove(file_b)

#         return filtered_file_path

#     except Exception as e:
#         # Supprimer les fichiers originaux en cas d'erreur
#         if os.path.exists(file_a):
#             os.remove(file_a)
#         if os.path.exists(file_b):
#             os.remove(file_b)
#         logging.error(f"Tâche échouée: {e}")
#         raise e

@celery.task(bind=True)
def full_outer_join_with_null(self, file_a, file_b, common_column):
    try:
        # Lire les fichiers CSV en utilisant des chunks optimisés
        df_a = dd.read_csv(file_a, dtype='object', blocksize='32MB')
        df_b = dd.read_csv(file_b, dtype='object', blocksize='32MB')

        # Nettoyer les noms de colonnes
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()

        # Vérifier si la colonne commune existe dans les deux fichiers
        if common_column not in df_a.columns or common_column not in df_b.columns:
            raise ValueError(f"La colonne commune '{common_column}' n'existe pas dans les deux fichiers.")

        # Fusionner les DataFrames avec une jointure extérieure complète sans suffixes
        df_merged = dd.merge(df_a, df_b, on=common_column, how='outer')

        # Log des données avant filtrage
        logging.info("Aperçu des données avant filtrage:")
        preview_before = df_merged.head(10)  # Obtenir un aperçu des 10 premières lignes
        logging.info(preview_before)

        # Filtrer les lignes où la clé est null dans l'une ou l'autre des tables
        df_filtered = df_merged[
            df_merged[common_column].isna() |
            df_merged[[col for col in df_merged.columns if col in df_a.columns and col != common_column]].isna().any(axis=1) |
            df_merged[[col for col in df_merged.columns if col in df_b.columns and col != common_column]].isna().any(axis=1)
        ]

        # Log des données après filtrage
        logging.info("Aperçu des données après filtrage:")
        preview_after = df_filtered.head(10)  # Obtenir un aperçu des 10 premières lignes
        logging.info(preview_after)

        # Remplacer les valeurs manquantes par 'null'
        df_filtered = df_filtered.fillna('null')

        # Générer un nom unique pour le fichier filtré
        filtered_file_path = f"/app/uploads/{os.path.basename(file_a)}_filtered.csv"
        df_filtered.compute().to_csv(filtered_file_path, index=False)

        # Supprimer les fichiers originaux
        os.remove(file_a)
        os.remove(file_b)

        return filtered_file_path

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_a):
            os.remove(file_a)
        if os.path.exists(file_b):
            os.remove(file_b)
        logging.error(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def remove_columns(self, file_path, columns_to_remove):
    try:
        # Lire le fichier CSV
        df = dd.read_csv(file_path,dtype='object')
        df.columns = df.columns.str.strip().str.lower()  # Nettoyer et convertir les noms de colonnes en minuscules

        # Si `columns_to_remove` est une liste, nettoyer les éléments
        if isinstance(columns_to_remove, list):
            #columns_to_remove = [col.strip('[]').strip('"').strip() for col in columns_to_remove]
            # Convertir la liste en une chaîne avec des virgules comme délimiteurs
            columns_to_remove = ','.join(columns_to_remove)
        elif isinstance(columns_to_remove, str):
            # Nettoyer la chaîne directement
            columns_to_remove = columns_to_remove.strip('[]').strip('"').strip()
        else:
            raise ValueError("Le paramètre columns_to_remove doit être une chaîne ou une liste.")

        # Séparer les noms des colonnes à supprimer en utilisant une expression régulière
        columns_to_remove = re.findall(r'\"(.*?)\"', columns_to_remove)
        cleaned_columns_to_remove = [col.strip().lower() for col in columns_to_remove if col.strip()]

        # Imprimer pour vérification
        print(f"Colonnes dans le DataFrame : {df.columns.tolist()}")
        print(f"Colonnes à supprimer : {columns_to_remove}")
        print(f"Colonnes à supprimer propre: {cleaned_columns_to_remove}")

        # Vérifier si les colonnes à supprimer existent dans le DataFrame
        missing_columns = [col for col in cleaned_columns_to_remove if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Les colonnes suivantes n'existent pas dans le fichier : {', '.join(missing_columns)}")

        # Supprimer les colonnes spécifiées
        df = df.drop(columns=cleaned_columns_to_remove)

        # Générer un nom unique pour le fichier résultant
        cleaned_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_path))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path = os.path.join(save_directory, cleaned_file_path)

        # Sauvegarder le DataFrame résultant dans le fichier
        df.to_csv(cleaned_file_path, index=False, single_file=True)
        if os.path.exists(file_path):
            os.remove(file_path)

        # Retourner le chemin du fichier résultant
        return cleaned_file_path

    except Exception as e:
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e
              
@celery.task(bind=True)
def extract_columns(self, file_path, columns_to_extract):
    try:
        # Lire le fichier CSV
        df = dd.read_csv(file_path,dtype='object')
        df.columns = df.columns.str.strip()  # Nettoyer les noms des colonnes

        # Vérifier si les colonnes à extraire existent dans le DataFrame
        missing_columns = [col for col in columns_to_extract if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Les colonnes suivantes n'existent pas dans le fichier : {', '.join(missing_columns)}")

        # Extraire les colonnes spécifiées
        df_extracted = df[columns_to_extract]
        
        # Générer un nom unique pour le nouveau fichier CSV
        extracted_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_path))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        extracted_file_path = os.path.join(save_directory, extracted_file_path)
        
        # Sauvegarder le DataFrame résultant dans le nouveau fichier CSV
        df_extracted.to_csv(extracted_file_path, index=False, single_file=True)
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        # Retourner le chemin du fichier résultant
        return extracted_file_path

    except Exception as e:
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e

@celery.task(bind=True)
def add_column_with_formula(self, file_path, new_column_name, formula):
    try:
        # Lire le fichier CSV
        df = dd.read_csv(file_path, dtype='object')
        df.columns = df.columns.str.strip()  # Nettoyer les noms des colonnes

        # Créer une fonction d'évaluation personnalisée pour les formules
        def apply_formula(row):
            # Remplacer les noms de colonnes dans la formule par les valeurs de la ligne
            formula_with_values = formula
            for col in df.columns:
                formula_with_values = formula_with_values.replace(f"{col}", f"row['{col}']")
            return eval(formula_with_values)

        # Appliquer la formule pour créer la nouvelle colonne
        df[new_column_name] = df.map_partitions(lambda df: df.apply(apply_formula, axis=1))

        # Générer un nom unique pour le nouveau fichier
        new_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_path))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        new_file_path = os.path.join(save_directory, new_file_path)

        # Sauvegarder le DataFrame avec la nouvelle colonne dans un fichier CSV
        df.to_csv(new_file_path, single_file=True, index=False)
        
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Retourner le chemin du nouveau fichier
        return new_file_path

    except Exception as e:
        # Supprimer le fichier original en cas d'erreur
        if os.path.exists(file_path):
            os.remove(file_path)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e

@celery.task
def delete_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        print(f"Erreur lors de la suppression du fichier : {e}")


# Fonctions de validation mises à jour
def validate_email(email, line_value):
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not isinstance(email, str):
        raise ValueError(f"Email non valide (non une chaîne) pour la valeur '{email}'")
    if not re.match(pattern, email):
        raise ValueError(f"Email non valide pour la valeur '{email}'")
    return email

def validate_number(value, line_value):
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"La valeur '{value}' n'est pas transformable en nombre")

def validate_integer(value, line_value):
    try:
        values=float(value)
        return int(values)
    except ValueError:
        raise ValueError(f"La valeur '{value}' à la ligne {line_value} n'est pas transformable en entier")

# Votre fonction validate_date reste inchangée
def validate_date(date_str, desired_format, line_value):
    possible_formats = [
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%d%m%Y", "%d %m %Y",
        "%m/%d/%Y", "%m-%d-%Y", "%m.%d.%Y", "%m%d%Y", "%m %d %Y",
        "%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d", "%Y%m%d",
        "%d/%m/%y", "%d-%m-%y", "%d.%m.%y", "%d%m%y", "%d %m %y",
        "%m/%d/%y", "%m-%d-%y", "%m.%d.%y", "%m%d%y", "%m %d %y",
        "%Y/%m/%y", "%Y-%m-%y", "%Y.%m.%y", "%Y%m%y",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", 
        "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M",
        "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M", 
        "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"
    ]
    
    if isinstance(date_str, str):
        for date_format in possible_formats:
            try:
                date_obj = datetime.strptime(date_str, date_format).date()
                return date_obj.strftime(desired_format)
            except ValueError:
                continue
    raise ValueError(f"La valeur '{date_str}' à la ligne {line_value} n'est pas transformable en date avec les formats disponibles.")

@celery.task(bind=True)
def clean_and_validate_data(self, file_path, validation_rules):
    try:
        # Lire le fichier CSV avec une taille de bloc pour un chargement paresseux
        df = dd.read_csv(file_path, dtype='object', blocksize='64MB')  # Ajustez la taille du bloc selon vos besoins
        df.columns = df.columns.str.strip()  # Nettoyer les noms des colonnes

        # Log des colonnes lues pour le débogage
        logger.info(f"Colonnes lues : {df.columns.tolist()}")

        # Ajouter une colonne d'index global
        df = df.reset_index()
        df = df.rename(columns={'index': 'global_index'})  # Correction ici

        # Appliquer les règles de validation et de nettoyage
        for rule in validation_rules:
            column = rule.get("column")
            actions = rule.get("actions", [])
            default_value = rule.get("default_value", None)
            date_format = rule.get("format", None)

            if column not in df.columns:
                raise ValueError(f"La colonne '{column}' n'existe pas dans le fichier.")

            logger.info(f"Application de l'action '{actions}' sur la colonne '{column}'")

            for action in actions:
                try:
                    if action == "validate_number":
                        df[column] = df[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_number(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_number_entier":
                        df[column] = df[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_integer(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_email":
                        df[column] = df[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_email(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_date":
                        if not date_format:
                            raise ValueError(f"Le format de date doit être spécifié pour l'action 'validate_date' sur la colonne '{column}'.")
                        
                        df[column] = df[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_date(x, date_format, column) if pd.notnull(x) else x),
                            meta=('x', 'object')
                        )
                    elif action == "strip_spaces":
                        df[column] = df[column].map_partitions(
                            lambda part: part.apply(
                                lambda x: re.sub(r'[^\w\s]', '', re.sub(r'\s+', ' ', str(x)).strip()) if not pd.isnull(x) else x,
                                meta=('x', 'object')
                            )
                        )
                    elif action == "capitalize":
                        df[column] = df[column].map_partitions(lambda part: part.str.lower(), meta=('x', 'object'))
                    elif action == "fill_missing":
                        df[column] = df[column].map_partitions(lambda part: part.fillna(default_value), meta=('x', 'object'))
                    else:
                        logger.warning(f"Action '{action}' non reconnue pour la colonne '{column}'")
                except Exception as e:
                    logger.error(f"Erreur lors de l'application de l'action '{action}' sur la colonne '{column}': {e}")
                    raise e

        # Réinitialiser l'index pour avoir un index global cohérent
        df = df.sort_values(by='global_index')  # Tri par index global
        df = df.reset_index(drop=True)  # Réinitialiser l'index après le tri
        # Supprimer la colonne d'index avant la sauvegarde
        df = df.drop(columns=['global_index'])

        # Générer un nom unique pour le fichier nettoyé
        cleaned_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_path))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path = os.path.join(save_directory, cleaned_file_path)
        
        # Sauvegarder le DataFrame nettoyé dans le fichier
        df.to_csv(cleaned_file_path, index=False, single_file=True)

        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)

        # Retourner le chemin du fichier nettoyé
        return cleaned_file_path

    except Exception as e:
        # Supprimer le fichier original en cas d'erreur
        if os.path.exists(file_path):
            os.remove(file_path)

        # Log de l'erreur pour des informations de débogage
        logger.error(f"Tâche échouée: {e}")

        # Relancer l'erreur pour le mécanisme de retry de Celery
        raise e

@celery.task(bind=True)
def clean_and_validate_data_old(self, file_path, validation_rules):
    try:
        # Lire le fichier CSV avec la première ligne comme en-tête
        df = dd.read_csv(file_path, dtype='object')
        df.columns = df.columns.str.strip()  # Nettoyer les noms des colonnes

        # Log des colonnes lues pour le débogage
        logger.info(f"Colonnes lues : {df.columns.tolist()}")

        # Ajouter une colonne d'index globale
        df = df.reset_index()
        df = df.rename(columns={'index': 'global_index'})  # Correction ici

        # Exclure la première ligne (en-tête) des opérations de validation
        df_data = df.reset_index(drop=True)  # Exclure la première ligne en utilisant `loc[1:]`

        # Appliquer les règles de validation et de nettoyage
        for rule in validation_rules:
            column = rule.get("column")
            actions = rule.get("actions", [])
            default_value = rule.get("default_value", None)
            date_format = rule.get("format", None)

            if column not in df_data.columns:
                raise ValueError(f"La colonne '{column}' n'existe pas dans le fichier.")

            logger.info(f"Application de l'action '{actions}' sur la colonne '{column}'")

            for action in actions:
                try:
                    if action == "validate_number":
                        df_data[column] = df_data[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_number(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_number_entier":
                        df_data[column] = df_data[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_integer(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_email":
                        df_data[column] = df_data[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_email(x, column)),
                            meta=('x', 'object')
                        )
                    elif action == "validate_date":
                        if not date_format:
                            raise ValueError(f"Le format de date doit être spécifié pour l'action 'validate_date' sur la colonne '{column}'.")
                        
                        # Appliquer la transformation sur chaque partition
                        df_data[column] = df_data[column].map_partitions(
                            lambda part: part.apply(lambda x: validate_date(x, date_format, column) if pd.notnull(x) else x),
                            meta=('x', 'object')
                        )
                    elif action == "strip_spaces":
                        # Log initial des données
                        logger.info(f"Entré dans strip_spaces pour la colonne '{column}' avec les 10 premières valeurs :\n{df_data[column].head(10)}")
    
                        # Transformation des valeurs de la colonne
                        df_data[column] = df_data[column].map_partitions(
                            lambda part: part.apply(
                                lambda x: re.sub(r'[^\w\s]', '', re.sub(r'\s+', ' ', str(x)).strip()) if not pd.isnull(x) else x
                            ),
                            meta=('x', 'object')
                        )
    
                        # Log final des données
                        logger.info(f"Sorti de strip_spaces pour la colonne '{column}' avec les 10 premières valeurs :\n{df_data[column].head(10)}")
                        logger.info(f"Type de la colonne après transformation : {df_data[column].dtype}")


                    elif action == "capitalize":
                        df_data[column] = df_data[column].map_partitions(lambda part: part.str.lower(), meta=('x', 'object'))
                    elif action == "fill_missing":
                        df_data[column] = df_data[column].map_partitions(lambda part: part.fillna(default_value), meta=('x', 'object'))
                    else:
                        logger.warning(f"Action '{action}' non reconnue pour la colonne '{column}'")
                except Exception as e:
                    logger.error(f"Erreur lors de l'application de l'action '{action}' sur la colonne '{column}': {e}")
                    raise e

        # Supprimer la colonne d'index avant la sauvegarde
        df_data = df_data.drop(columns=['global_index'])

        # Générer un nom unique pour le fichier nettoyé
        cleaned_file_path = FileNameGenerator.generate_unique_filename(os.path.basename(file_path))
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path = os.path.join(save_directory, cleaned_file_path)
        
        # Sauvegarder le DataFrame nettoyé dans le fichier
        df_data.to_csv(cleaned_file_path, index=False, single_file=True)

        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)

        # Retourner le chemin du fichier nettoyé
        return cleaned_file_path

    except Exception as e:
        # Supprimer le fichier original en cas d'erreur
        if os.path.exists(file_path):
            os.remove(file_path)

        # Log de l'erreur pour des informations de débogage
        logger.error(f"Tâche échouée: {e}")

        # Relancer l'erreur pour le mécanisme de retry de Celery
        raise e

@celery.task(bind=True)
def validate_urls_in_csv(self, file_path, url_column_name):
    try:
        # Lire le fichier CSV en utilisant Dask avec un index global
        df = dd.read_csv(file_path,dtype='object')

        # Assurez-vous que le nom de colonne est propre
        df.columns = df.columns.str.strip()

        # Vérifiez si la colonne URL existe dans le DataFrame
        if url_column_name not in df.columns:
            raise ValueError(f"La colonne '{url_column_name}' n'existe pas dans le fichier.")

        # Fonction pour vérifier la validité d'une URL
        def is_valid_url(url):
            regex = re.compile(
                r'^(?:http|ftp)s?://' # http:// ou https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domaine
                r'localhost|' # localhost
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # adresse IP
                r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ou IPv6
                r'(?::\d+)?' # port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            return re.match(regex, url) is not None

        # Fonction pour vérifier les URLs dans une partition
        def validate_partition(partition, url_column_name):
            # Assurez-vous que la colonne URL existe dans cette partition
            if url_column_name not in partition.columns:
                raise ValueError(f"La colonne '{url_column_name}' n'existe pas dans la partition.")
            
            # Vérifier la validité des URLs
            invalid_rows = partition[~partition[url_column_name].map(is_valid_url)]
            
            return invalid_rows

        # Ajouter un index global avant de diviser en partitions
        df['global_index'] = df.index

        # Appliquer la validation sur chaque partition et conserver les indices globaux
        invalid_df = df.map_partitions(lambda partition: validate_partition(partition, url_column_name)).compute()

        if not invalid_df.empty:
            # Réinitialiser l'index pour avoir un index global cohérent
            invalid_df = invalid_df.reset_index(drop=True)
            invalid_df['global_index'] = invalid_df['global_index']  +1 # Correction des indices pour commencer à 1

            # Enregistrer les résultats invalides dans un fichier
            # invalid_urls_file = 'invalid_urls.csv'
            # invalid_df.to_csv(invalid_urls_file, index=False)
            
            # Préparer le message d'erreur avec les détails
            invalid_df['global_index'] = invalid_df['global_index'] +1
            error_message = f"URLs invalides trouvées."
            error_details = invalid_df[['global_index', url_column_name ]].to_string(index=False)
            # Supprimer le fichier original
            if os.path.exists(file_path):
                os.remove(file_path)
            raise ValueError(f"{error_message}\nDétails des erreurs :\n{error_details}")
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        return "Toutes les URLs sont valides."

    except Exception as e:
        # Supprimer le fichier original
        if os.path.exists(file_path):
            os.remove(file_path)
        print(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def get_entries_by_cookie_and_state_task(self,cookie_id, etat):
    """Tâche Celery pour récupérer les entrées par id_cookie et état."""
    try:
        # Récupérer les entrées
        entries, total_count = TableauDeBord.get_entries_by_cookie_and_state(cookie_id, etat)

        # Créer une entrée pour indiquer le succès de la récupération
        # entry_data = {
        #     'id_cookie': cookie_id,
        #     'mouvement': 'get_entries',
        #     'nom_du_fichier': 'N/A',  # Aucune valeur de fichier spécifiée
        #     'etat': 'success',
        #     'task': None
        # }

        # Appel de la méthode add_entry pour enregistrer l'action
        #TableauDeBord.add_entry(entry_data)

        # Retourner les résultats
        return {
            'entries': [entry.to_dict() for entry in entries],  # Convertir en dictionnaire si nécessaire
            'total_count': total_count
        }
    except Exception as e:
        # En cas d'erreur, enregistrer l'entrée avec état "error"
        # entry_data = {
        #     'id_cookie': cookie_id,
        #     'mouvement': 'get_entries',
        #     'nom_du_fichier': 'N/A',
        #     'etat': 'error',
        #     'task': None
        # }

        # TableauDeBord.add_entry(entry_data)

        return {'error': f"Erreur lors de la récupération des entrées : {str(e)}"}, 500

@celery.task(bind=True)
def detect_outliers(self, file_path, age_column):
     try:
        # Lire le fichier CSV avec Dask
        df = dd.read_csv(file_path, dtype='object', blocksize='32MB')

        # Vérifier si la colonne d'âge existe dans le DataFrame
        if age_column not in df.columns:
            raise ValueError(f"La colonne d'âge '{age_column}' n'existe pas dans le fichier.")

        # Convertir la colonne d'âge en type numérique, en forçant les valeurs non numériques à NaN
        df[age_column] = dd.to_numeric(df[age_column], errors='coerce')

        # Filtrer les lignes avec des âges numériques
        df = df[~df[age_column].isna()]

        # Calculer les quartiles et l'IQR pour la colonne d'âge
        Q1 = df[age_column].quantile(0.25).compute()
        Q3 = df[age_column].quantile(0.75).compute()
        IQR = Q3 - Q1

        # Définir les bornes pour détecter les outliers
        borne_inferieure = Q1 - 1.5 * IQR
        borne_superieure = Q3 + 1.5 * IQR

        # Filtrer les outliers pour les âges
        outliers = df[
            (df[age_column] < borne_inferieure) |
            (df[age_column] > borne_superieure) |
            (df[age_column] < 0)   # Vérifie si ce n'est pas un entier
        ].compute()

        # Générer un rapport avec les lignes complètes des outliers
        outlier_report = outliers.to_dict('records')

        # Retourner le rapport des outliers
        return {"outliers": outlier_report}

     except Exception as e:
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def detect_age_outliers(self, file_path, age_column):
    try:
        # Lire le fichier CSV avec Dask
        df = dd.read_csv(file_path, dtype='object', blocksize='32MB')

        # Vérifier si la colonne d'âge existe dans le DataFrame
        if age_column not in df.columns:
            raise ValueError(f"La colonne d'âge '{age_column}' n'existe pas dans le fichier.")

        # Convertir la colonne d'âge en type numérique, en forçant les valeurs non numériques à NaN
        df[age_column] = dd.to_numeric(df[age_column], errors='coerce')

        # Filtrer les lignes avec des âges numériques
        df = df[~df[age_column].isna()]

        # Calculer les quartiles et l'IQR pour la colonne d'âge
        Q1 = df[age_column].quantile(0.25).compute()
        Q3 = df[age_column].quantile(0.75).compute()
        IQR = Q3 - Q1

        # Définir les bornes pour détecter les outliers
        borne_inferieure = Q1 - 1.5 * IQR
        borne_superieure = Q3 + 1.5 * IQR

        # Filtrer les outliers pour les âges
        outliers = df[
            (df[age_column] < borne_inferieure) |
            (df[age_column] > borne_superieure) |
            (df[age_column] < 0) |
            (df[age_column] % 1 != 0)  # Vérifie si ce n'est pas un entier
        ].compute()

        # Générer un rapport avec les lignes complètes des outliers
        outlier_report = outliers.to_dict('records')

        # Retourner le rapport des outliers
        return {"outliers": outlier_report}

    except Exception as e:
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def detect_non_numeric_values(self, file_path, target_column):
    try:
        # Lire le fichier CSV avec Dask
        df = dd.read_csv(file_path, dtype='object', blocksize='64MB')

        # Vérifier si la colonne cible existe dans le DataFrame
        if target_column not in df.columns:
            raise ValueError(f"La colonne '{target_column}' n'existe pas dans le fichier.")

        # Identifier les lignes où la colonne cible est non numérique
        non_numeric_rows = df[~df[target_column].str.isnumeric()]

        # Extraire les valeurs non numériques
        non_numeric_values = non_numeric_rows[target_column].compute().tolist()
        non_numeric_indices = non_numeric_rows.index.compute().tolist()

        # Générer un rapport avec les valeurs non numériques
        non_numeric_report = {
            "non_numeric_values": non_numeric_values,
            "count": len(non_numeric_values),
            "indices": non_numeric_indices
        }

        # Retourner le rapport des valeurs non numériques
        return non_numeric_report

    except Exception as e:
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée: {e}")
        raise e

@celery.task(bind=True)
def compare_two_csv_nope(self, file_path1, file_path2):
    try:
        # Lire les deux fichiers CSV avec Dask
        df1 = dd.read_csv(file_path1, encoding='ISO-8859-1', dtype='object').applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df2 = dd.read_csv(file_path2, encoding='ISO-8859-1', dtype='object').applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Identifier les lignes communes
        common_rows = df1.merge(df2, how='inner')

        # Convertir les DataFrames Dask en DataFrames pandas pour le surlignage
        df1_pd = df1.compute()
        df2_pd = df2.compute()

        # Créer un ensemble de tuples pour les lignes communes
        common_rows_set = set(tuple(row) for row in common_rows.to_numpy())

        # Fonction pour surligner les lignes communes
        def highlight_common_rows(row):
            return ['background-color: #74cec8; color: #fff' if tuple(row) in common_rows_set else '' for _ in row]

        # Appliquer le surlignage sur les DataFrames pandas
        df1_highlighted = df1_pd.style.apply(highlight_common_rows, axis=1)
        df2_highlighted = df2_pd.style.apply(highlight_common_rows, axis=1)

        # Générer des noms uniques pour les fichiers nettoyés
        cleaned_file_path1 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path1), extension='html')
        cleaned_file_path2 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path2), extension='html')

        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path1 = os.path.join(save_directory, cleaned_file_path1)
        cleaned_file_path2 = os.path.join(save_directory, cleaned_file_path2)

        # Sauvegarder les fichiers nettoyés avec les surlignages
        df1_highlighted.to_html(cleaned_file_path1, index=False, escape=False)
        df2_highlighted.to_html(cleaned_file_path2, index=False, escape=False)

        # Supprimer les fichiers originaux
        os.remove(file_path1)
        os.remove(file_path2)

        # Retourner les chemins des fichiers surlignés
        return cleaned_file_path1, cleaned_file_path2

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée à cause de: {e}")
        self.update_state(
            state='FAILURE',
            meta={'exc_type': type(e).__name__, 'exc_message': str(e)}
        )
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e


@celery.task(bind=True)
def compare_two_csv(self, file_path1, file_path2):
    try:
        # Lire les deux fichiers CSV
        df1 = dd.read_csv(file_path1, encoding='ISO-8859-1', dtype='object')
        df2 = dd.read_csv(file_path2, encoding='ISO-8859-1', dtype='object')

        # Convertir les DataFrames Dask en DataFrames pandas pour la comparaison
        df1_pd = df1.compute().apply(lambda x: x.str.strip())
        df2_pd = df2.compute().apply(lambda x: x.str.strip())

        # Créer un ensemble de tuples pour chaque DataFrame représentant les lignes comme des ensembles de valeurs
        set_df1 = set(tuple(sorted(row)) for row in df1_pd.values)
        set_df2 = set(tuple(sorted(row)) for row in df2_pd.values)

        # Identifier les lignes communes entre les deux fichiers
        common_rows_set = set_df1.intersection(set_df2)

        # Fonction pour surligner les lignes communes
        def highlight_common_rows(row, common_rows_set):
            return ['background-color: #74cec8d0;color:  #fff' if tuple(sorted(row)) in common_rows_set else '' for _ in row]

        # Appliquer le surlignage des lignes communes dans chaque DataFrame
        df1_highlighted = df1_pd.style.apply(highlight_common_rows, common_rows_set=common_rows_set, axis=1)
        df2_highlighted = df2_pd.style.apply(highlight_common_rows, common_rows_set=common_rows_set, axis=1)

        # Générer des noms uniques pour les fichiers nettoyés
        cleaned_file_path1 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path1), extension='html')
        cleaned_file_path2 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path2), extension='html')
        
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path1 = os.path.join(save_directory, cleaned_file_path1)
        cleaned_file_path2 = os.path.join(save_directory, cleaned_file_path2)

        # Sauvegarder les fichiers nettoyés avec les surlignages
        df1_highlighted.to_html(cleaned_file_path1, index=False, escape=False)
        df2_highlighted.to_html(cleaned_file_path2, index=False, escape=False)

        # Supprimer les fichiers originaux
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        
        # Retourner les chemins des fichiers surlignés
        return cleaned_file_path1, cleaned_file_path2

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée à cause de: {e}")
        self.update_state(
            state='FAILURE',
            meta={'exc_type': type(e).__name__, 'exc_message': str(e)}
        )
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e
        
def compare_two_csv_old(self, file_path1, file_path2):
    try:
        # Lire les deux fichiers CSV
        df1 = dd.read_csv(file_path1, encoding='ISO-8859-1', dtype='object')
        df2 = dd.read_csv(file_path2, encoding='ISO-8859-1', dtype='object')

        # Convertir les DataFrames Dask en DataFrames pandas pour comparaison
        df1_pd = df1.compute()
        df2_pd = df2.compute()

        # Identifier les lignes communes
        common_rows = pd.merge(df1_pd, df2_pd, how='inner', on=list(df1_pd.columns))

        # Fonction pour surligner les lignes communes
        def highlight_common_rows(row, common_rows_set):
            return ['background-color: #ffcccc' if tuple(row) in common_rows_set else '' for _ in row]

        # Appliquer le surlignage des lignes communes
        common_rows_set = set(tuple(row) for row in common_rows.to_records(index=False))
        df1_highlighted = df1_pd.style.apply(highlight_common_rows, common_rows_set=common_rows_set, axis=1)
        df2_highlighted = df2_pd.style.apply(highlight_common_rows, common_rows_set=common_rows_set, axis=1)

        # Générer des noms uniques pour les fichiers nettoyés
        cleaned_file_path1 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path1), extension='html')
        cleaned_file_path2 = FileNameGenerator.generate_unique_filename(os.path.basename(file_path2), extension='html')
        
        save_directory = '/app/uploads'  # Spécifiez votre répertoire de sauvegarde
        cleaned_file_path1 = os.path.join(save_directory, cleaned_file_path1)
        cleaned_file_path2 = os.path.join(save_directory, cleaned_file_path2)

        # Sauvegarder les fichiers nettoyés avec les surlignages
        df1_highlighted.to_html(cleaned_file_path1, index=False, escape=False)
        df2_highlighted.to_html(cleaned_file_path2, index=False, escape=False)

        # Supprimer les fichiers originaux
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        
        # Mettre à jour l'état de la tâche avec les résultats
        #self.update_state(state='SUCCESS', meta={'result': (cleaned_file_path1, cleaned_file_path2)})
        return cleaned_file_path1, cleaned_file_path2

    except Exception as e:
        # Supprimer les fichiers originaux en cas d'erreur
        if os.path.exists(file_path1):
            os.remove(file_path1)
        if os.path.exists(file_path2):
            os.remove(file_path2)
        # Log de l'erreur pour des informations de débogage
        print(f"Tâche échouée à cause de: {e}")
        self.update_state(
            state='FAILURE',
            meta={'exc_type': type(e).__name__, 'exc_message': str(e)}
        )
        # Ne pas relancer l'erreur pour le mécanisme de retry de Celery
        raise e




