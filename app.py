# app.py
from flask import Flask, request, jsonify, send_file, make_response,  render_template
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from celery.result import AsyncResult
from datetime import datetime, timedelta
from io import BytesIO
from models import User, FileNameGenerator , TableauDeBord, Metadata
from db_manager import get_session, close_session
from extensions import db
from celery_config import make_celery
from celery_tasks import  compare_two_csv, detect_non_numeric_values, detect_age_outliers, detect_outliers, delete_folder_contents, remove_duplicates_key, clean_csv, clean_two_csv, Find_A_and_B_middle, find_A_throw_B, find_A_fill_B, full_outer_join,full_outer_join_with_null,delete_file,clean_and_validate_data,add_column_with_formula,extract_columns,remove_columns,validate_urls_in_csv,get_entries_by_cookie_and_state_task
import os
import tempfile
import pandas as pd
import dask.dataframe as dd
import zipfile
import re
import json
from datetime import datetime,date
import uuid
import logging

# Configurer le logger
logging.basicConfig(level=logging.INFO)  # Niveau de log INFO, ajustez selon vos besoins
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Configurer CORS pour permettre les cookies
frontend_url = os.getenv("FRONTEND_URL", "https://csv-analyzer-two.vercel.app")
CORS(app, supports_credentials=True, resources={r"/*": {"origins": frontend_url}})
# Configurer Celery/Configurer SQLAlchemy
app.config['broker_url'] = 'amqp://z28SN5429pup5xwC:pLm83EcfbnI76Ktpz-yke9QmZp4ScyjL@autorack.proxy.rlwy.net:16856/%2F'
app.config['result_backend'] = 'redis://default:eoLLKKdgGxhfcOtaBGJEqWjcQPXUJEnW@junction.proxy.rlwy.net:25101/0'# os.getenv('CELERY_RESULT_BACKEND')#
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:yxhPmwwwpisNQMfWGUeHTlEkdWDRSkLp@junction.proxy.rlwy.net:32235/railway'#os.getenv('SQLALCHEMY_DATABASE_URI')
db.init_app(app)


celery = make_celery(app) 

from celery.schedules import crontab
celery.conf.beat_schedule = {
    'delete-at-17-00-everyday': {
        'task': 'celery_tasks.delete_folder_contents',
        'schedule': crontab(hour=11, minute=20),  # Tous les jours à 17h
        'args': ('/app/uploads',)
    },
}

@app.route('/')
def index():
    
    # Lire le cookie existant
    cookie_id = request.cookies.get('cookie_id')
    # Créer une réponse HTTP
    resp = make_response('Bienvenue sur le site')
    logging.info(f"Le premier cookie_id {cookie_id}")
    if cookie_id is None:
        # Le cookie n'existe pas encore, donc le créer
        unique_id = str(uuid.uuid4())  # Générer un identifiant unique
        expires = datetime.utcnow() + timedelta(days=30)
        resp.set_cookie('cookie_id', unique_id, expires=expires, httponly=True, secure=True, samesite='Lax')
        message = ' Un nouveau cookie a été créé.'
    else:
        # Le cookie existe déjà, donc il est considéré comme valide
        message = ' Votre cookie est valide.'

    # Inclure le message d'information dans la réponse
    resp.set_data(f'{resp.get_data(as_text=True)} {message}')
    
    # Afficher le cookie_id dans la console
    print(f'Valeur du cookie_id: {cookie_id}')
    
    return resp

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files' not in request.files:
        return 'Aucun fichier trouvé dans la requête', 400

    files = request.files.getlist('files')
    if not files:
        return 'Aucun fichier à uploader', 400

    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return 'Cookie non trouvé', 400

    # Sauvegarder les fichiers dans le dossier 'uploads' et enregistrer dans la base de données
    os.makedirs('uploads', exist_ok=True)
    print(cookie_id)
    for file in files:
        file_path = os.path.join('uploads', file.filename)
        file.save(file_path)
        
        # Sauvegarder le nom du fichier et l'ID du cookie dans la base de données
        # file_entry = FileUpload(filename=file.filename, cookie_id=cookie_id)
        # db.session.add(file_entry)
        # db.session.commit()

    return 'Fichiers uploadés avec succès et informations sauvegardées', 200

@app.route('/add/<int:a>/<int:b>', methods=['GET'])
def add_task(a, b):
    task = add.delay(a, b)
    result = task.get(timeout=10)  # Attendre jusqu'à 10 secondes pour le résultat
    return jsonify({'task_id': task.id, 'result': result}), 200
@app.route('/task_status/<task_id>', methods=['GET'])
def task_status(task_id):
    task = AsyncResult(task_id, app=celery)
    
    response = {
        'state': task.state,
        'current': 0,
        'total': 1,
        'result': None,
        'status': 'En attente...'
    }

    try:
        if task.state == 'PENDING':
            response['status'] = 'En attente...'
        elif task.state == 'FAILURE':
            response['current'] = 1
            try:
                response['status'] = str(task.info) if task.info else 'Erreur inconnue'
            except AttributeError:
                response['status'] = 'Erreur inconnue'
            
            # Mettre à jour l'état de la tâche en 'fail'
            entry = TableauDeBord.query.filter_by(task=task_id).first()
            if entry:
                entry.etat = 'fail'
                db.session.commit()

        elif task.state == 'SUCCESS':
            response['status'] = 'Succès'
            response['result'] = task.result  # Assurez-vous que c'est sérialisable
            
            # Mettre à jour l'état de la tâche en 'succes'
            entry = TableauDeBord.query.filter_by(task=task_id).first()
            if entry:
                entry.etat = 'succes'
                db.session.commit()

        else:
            if isinstance(task.info, dict):
                response.update({
                    'current': task.info.get('current', 0),
                    'total': task.info.get('total', 1),
                    'status': task.info.get('status', '')
                })
                if 'result' in task.info:
                    response['result'] = task.info['result']
            elif isinstance(task.info, list):
                response.update({
                    'current': task.info[0] if len(task.info) > 0 else 0,
                    'total': task.info[1] if len(task.info) > 1 else 1,
                    'status': task.info[2] if len(task.info) > 2 else ''
                })
                if len(task.info) > 3:
                    response['result'] = task.info[3]
            else:
                response['status'] = 'Information de tâche inconnue'

    except WorkerLostError as e:
        response.update({
            'state': 'FAILURE',
            'status': 'Worker lost',
            'error': str(e)
        })
        entry = TableauDeBord.query.filter_by(task=task_id).first()
        if entry:
            entry.etat = 'fail'
            db.session.commit()
    except Exception as e:
        print(f"Erreur lors de la mise à jour de l'état de la tâche : {e}")
        db.session.rollback()
        response.update({
            'state': 'FAILURE',
            'status': 'Erreur serveur',
            'error': str(e)
        })

    return jsonify(response)

def task_status_old(task_id):
    task = AsyncResult(task_id, app=celery)
    
    # Définir le message de réponse
    response = {
        'state': task.state,
        'current': 0,
        'total': 1,
        'result': task.result ,
        'status': 'En attente...'
    }
    
    # Mettre à jour la ligne dans la base de données
    try:
        if task.state == 'PENDING':
            response['status'] = 'En attente...'
        elif task.state == 'FAILURE':
            response['current'] = 1
            response['status'] = str(task.info)  # Exception raised
            
            # Mettre à jour l'état de la tâche en 'fail'
            entry = TableauDeBord.query.filter_by(task=task_id).first()
            if entry:
                entry.etat = 'fail'
                db.session.commit()
                
        elif task.state == 'SUCCESS':
            response['status'] = 'Succès'
            
            # Mettre à jour l'état de la tâche en 'succes'
            entry = TableauDeBord.query.filter_by(task=task_id).first()
            if entry:
                entry.etat = 'succes'
                db.session.commit()
        
        else:
            if isinstance(task.info, dict):
                response.update({
                    'current': task.info.get('current', 0),
                    'total': task.info.get('total', 1),
                    'status': task.info.get('status', '')
                })
                if 'result' in task.info:
                    response['result'] = task.info['result']
            elif isinstance(task.info, list):
                response.update({
                    'current': task.info[0] if len(task.info) > 0 else 0,
                    'total': task.info[1] if len(task.info) > 1 else 1,
                    'status': task.info[2] if len(task.info) > 2 else ''
                })
                if len(task.info) > 3:
                    response['result'] = task.info[3]
            else:
                response['status'] = 'Information de tâche inconnue'
    
    except Exception as e:
        print(f"Erreur lors de la mise à jour de l'état de la tâche : {e}")
        db.session.rollback()

    return jsonify(response)
    
@app.route('/users', methods=['GET'])
def get_users():# Obtenir tous les utilisateurs depuis la base de données
    session = get_session()
    print('haha')
    try:
        users = User.get_all_users()
        print('haha3')
        user_list = [User.to_dict(user) for user in users]
        return jsonify(user_list), 200
    finally:
        close_session(session)  # Assurez-vous que la session est fermée

@app.route('/clean_csv', methods=['POST'])
def clean_csv_route():
    # Vérifiez si 'files' est dans la requête
    if 'files' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': 'Aucun fichier à uploader'}), 400

    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    os.makedirs('uploads', exist_ok=True)
    
    try:
        # Traitez tous les fichiers envoyés
        for file in files:
            if file.filename == '':
                return jsonify({'error': 'Nom de fichier invalide'}), 400

            # Sauvegarde du fichier dans un répertoire temporaire avec un nom unique
            temp_file_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file.filename))
            file.save(temp_file_path)
            

            # Obtenir le poids du fichier en Mo
            poids = os.path.getsize(temp_file_path) / (1024 * 1024)  # Convertir en Mo

            # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
            # df = dd.read_csv(temp_file_path)
            # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
            # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
            nombre_de_lignes, nombre_de_colonnes = count_rows_columns(temp_file_path)
            # Ajouter une entrée à la fin avec état "success"
            entry_data = {
                'id_cookie': cookie_id,
                'mouvement': 'Nettoyage unique',
                'nom_du_fichier': file.filename,
                'etat': 'interrupted',
                'task':None,
                'nombre_de_ligne': nombre_de_lignes
            }
            #ICI
             # Préparer les données pour la table metadata
            metadata_entry = {
                'id_cookie': cookie_id,
                'nom_du_fichier': file.filename,
                'nombre_de_colonnes': nombre_de_colonnes,
                'nombre_de_lignes': nombre_de_lignes,
                'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
                'poids': poids,
                'unite': 'Mo',  # Unité en Mo
                'action': 'Nettoyage unique'  # Action
            }

            # Ajoutez l'entrée à la table metadata
            metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
            db.session.add(metadata)  # Ajoutez à la session
            db.session.commit()  # Validez la transaction
            # Appel de la méthode add_entry
            #entry = TableauDeBord(**entry_data)
            entry = TableauDeBord.add_entry(entry_data)
            # Lancement de la tâche Celery pour nettoyer le CSV
            task = clean_csv.delay(temp_file_path)
            # Mettre à jour l'entrée avec l'ID de la tâche Celery
            entry.task = task.id
            db.session.commit()
        download_url = f"http://127.0.0.1:5000/download_clean_csv/{task.id}"
        
        
        
        # Retournez un message de succès
        return jsonify({
            'message': f"Tâche en cours pour nettoyer les fichiers CSV.",
            'task_id': task.id,
            'download_url': download_url
        })
    except Exception as e: 
        # Gérez les exceptions et retournez une réponse d'erreur
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


@app.route('/download_clean_csv/<task_id>', methods=['GET'])
def download_clean_csv(task_id):
    # Récupérer le résultat de la tâche Celery
    result = celery.AsyncResult(task_id)
    
    if result.ready() and not result.failed():
        cleaned_file_path = result.get()
        
        # Vérifiez si le fichier existe avant de tenter de l'envoyer
        if not os.path.exists(cleaned_file_path):
            return jsonify({'error': 'Le fichier nettoyé n\'existe pas.'}), 404

        # Télécharger le fichier temporaire
        try:
            response = send_file(cleaned_file_path, as_attachment=True, download_name='cleaned_data.csv')
            # Lancer une tâche Celery pour supprimer le fichier temporaire après téléchargement
            delete_file.apply_async((cleaned_file_path,), countdown=300)  # Supprimer après 5 minutes
            return response
        finally:
            # Si quelque chose échoue, assurez-vous de supprimer le fichier temporaire
            if os.path.exists(cleaned_file_path):
                os.remove(cleaned_file_path)
    
    return jsonify({'error': 'La tâche n\'est pas encore terminée ou a échoué'}), 400

@app.route('/clean_csv_duplicate_key', methods=['POST'])
def clean_csv_duplicate_key_route():
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    common_column = request.form.get('common_column')
    if common_column is None:
        return jsonify({'error': 'Le nom de la colonne commune est requis'}), 400

    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    os.makedirs('uploads', exist_ok=True)

    try:
        temp_file_path = os.path.join('uploads', file.filename)
        file.save(temp_file_path)

        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(temp_file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(temp_file_pat,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(temp_file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Respect cle primaire'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Respect cle primaire',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        entry = TableauDeBord.add_entry(entry_data)
        # Lancer la tâche Celery pour nettoyer le CSV
        task = remove_duplicates_key.delay(temp_file_path, common_column)
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()
        
        return jsonify({
            'message': f"Tâche en cours pour nettoyer le fichier CSV {file.filename}",
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


@app.route('/clean_two_csv', methods=['POST'])
def clean_two_csv_route():
    # Vérifiez la présence du cookie
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400
    
    # Assurez-vous que le répertoire de téléchargement existe
    os.makedirs('uploads', exist_ok=True)

    # Vérifiez les fichiers dans la requête
    if 'files' not in request.files:
        return jsonify({'error': 'Both files are required'}), 400

    files = request.files.getlist('files')
    
    if len(files) != 2:
        return jsonify({'error': 'Exactly two files are required'}), 400

    file1 = files[0]
    file2 = files[1]
    
    if file1.filename == '' or file2.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    try:
        # Sauvegarder les fichiers dans le répertoire de téléchargement
        file1_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file1.filename))
        file2_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file2.filename))
        
        file1.save(file1_path)
        file2.save(file2_path)
        
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file1_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file1_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file1_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file1.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Nettoyage multiple'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file2_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file2_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_ligne, nombre_de_colonnes = count_rows_columns(file2_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file2.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_ligne,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Nettoyage multiple'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Nettoyage multiple',
            'nom_du_fichier': file1.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)
        # Lancer la tâche Celery pour nettoyer les fichiers CSV
        task = clean_two_csv.apply_async((file1_path, file2_path))
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()
        return jsonify({
            'message': f"Tâche en cours pour nettoyer les fichiers CSV : {file1_path} et {file2_path}",
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement des fichiers : {str(e)}"}), 500



@app.route('/download_cleaned_csv/<task_id>', methods=['GET'])
def download_clean_two_csv(task_id):
    task = clean_csv.AsyncResult(task_id)
    
    if task.ready() and not task.failed():
        cleaned_file_path1, cleaned_file_path2 = task.get()
         # Créer une archive ZIP contenant les fichiers nettoyés
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(cleaned_file_path1, os.path.basename(cleaned_file_path1))
            zip_file.write(cleaned_file_path2, os.path.basename(cleaned_file_path2))
        
        zip_buffer.seek(0)
        
        # Télécharger l'archive ZIP
        try:
            response = send_file(zip_buffer, as_attachment=True, download_name='cleaned_data.zip')
            
            # Lancer une tâche Celery pour supprimer les fichiers temporaires après téléchargement
            delete_file.apply_async((cleaned_file_path1,), countdown=300)  
            delete_file.apply_async((cleaned_file_path2,), countdown=300)  
            
            return response
        finally:
            if os.path.exists(cleaned_file_path1):
                os.remove(cleaned_file_path1)
            if os.path.exists(cleaned_file_path2):
                os.remove(cleaned_file_path2)
    
    return jsonify({'error': 'La tâche n\'est pas encore terminée ou a échoué'}), 400


@app.route('/Fusion_csv_files', methods=['GET'])
def Fusion_csv_files_route():
    # if 'file1' not in request.files or 'file2' not in request.files:
    #     return jsonify({'error': 'Both files are required'}), 400

    # file1 = request.files['file1']
    # file2 = request.files['file2']
    # common_column = request.form.get('common_column')

    # if not common_column:
    #     return jsonify({'error': 'Common column name is required'}), 400
    file_path1 = '/app/fichier.csv' 
    file_path2 = '/app/fichier2.csv'
    common_column = 'Colonne1'
    try:
        # Sauvegarder les fichiers dans le répertoire temporaire
        # file_path1 = os.path.join(TEMP_DIR, generate_unique_filename(file1.filename))
        # file_path2 = os.path.join(TEMP_DIR, generate_unique_filename(file2.filename))
        
        # file1.save(file_path1)
        # file2.save(file_path2)
        
        # Lancement de la tâche Celery pour fusionner les fichiers CSV
        task = full_outer_join.delay(file_path1, file_path2, common_column)
        
        return jsonify({
            'message': f"Tâche en cours pour fusionner les fichiers CSV : {file_path1} et {file_path2}",
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement des fichiers : {str(e)}"}), 500


@app.route('/manipulate_two_csv', methods=['POST'])
def manipulate_two_csv_route():
    try:
        # Vérifiez la présence du cookie
        cookie_id = request.cookies.get('cookie_id')
        if cookie_id is None:
            return jsonify({'error': 'Cookie non trouvé'}), 400

        # Assurez-vous que le répertoire de téléchargement existe
        os.makedirs('uploads', exist_ok=True)

        # Vérifiez les fichiers dans la requête
        if 'files' not in request.files:
            return jsonify({'error': 'Both files are required'}), 400

        files = request.files.getlist('files')

        if len(files) < 2:
            return jsonify({'error': 'More than one file is required'}), 400

        file1 = files[0]
        file2 = files[1]

        if file1.filename == '' or file2.filename == '':
            return jsonify({'error': 'No selected file'}), 400

        # Récupérer l'indice de la requête
        index = request.form.get('index')
        if index is None:
            return jsonify({'error': 'Index is required'}), 400

        # Récupérer le nom de la colonne de la requête
        common_column = request.form.get('common_column')
        if common_column is None:
            return jsonify({'error': 'Common column is required'}), 400

        try:
            index = int(index)
            if index < 1 or index > 5:
                return jsonify({'error': 'Index must be between 1 and 5'}), 400
        except ValueError:
            return jsonify({'error': 'Index must be an integer'}), 400

        # Sauvegarder les fichiers dans le répertoire de téléchargement
        file1_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file1.filename))
        file2_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file2.filename))

        file1.save(file1_path)
        file2.save(file2_path)

        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file1_path)
        mouvement = 'error'
        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': mouvement,
            'nom_du_fichier': file1.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)
        # Définir la tâche en fonction de l'indice
        if index == 1:
            task = Find_A_and_B_middle.apply_async((file1_path, file2_path, common_column))
            mouvement = 'Find_A_and_B_middle'
        elif index == 2:
            task = find_A_throw_B.apply_async((file1_path, file2_path, common_column))
            mouvement = 'find_A_throw_B'
        elif index == 3:
            task = find_A_fill_B.apply_async((file1_path, file2_path, common_column))
            mouvement = 'find_A_fill_B'
        elif index == 4:
            task = full_outer_join.apply_async((file1_path, file2_path, common_column))
            mouvement = 'full_outer_join'
        elif index == 5:
            task = full_outer_join_with_null.apply_async((file1_path, file2_path, common_column))
            mouvement = 'full_outer_join_with_null'
        else:
            return jsonify({'error': 'Invalid index'}), 400

        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file1_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file1_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file1.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': mouvement  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file2_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file2_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file2_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file2.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': mouvement  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        entry.mouvement = mouvement
        db.session.commit()
        # Attendre la complétion de la tâche
        task_id = task.id
        task_result = task.get()  # Timeout de 5 minutes pour éviter le blocage

        # Vérifiez si la tâche a échoué
        if task.state == 'FAILURE':
            error_message = task.result or 'Tâche échouée avec une erreur inconnue.'
            return jsonify({'error': f"Tâche échouée: {error_message}"}), 500

        # Vérifiez le résultat de la tâche
        if isinstance(task_result, dict) and task_result.get('status') == 'error':
            return jsonify({'error': task_result.get('message', 'Erreur inconnue')}), 400

        return jsonify({
            'message': f"Tâche en cours pour manipuler les fichiers CSV : {file1_path} et {file2_path} avec l'indice {index}",
            'task_id': task_id
        })

    except Exception as e:
        app.logger.error(f"Erreur lors du traitement des fichiers : {str(e)}")
        return jsonify({'error': f" {str(e)}"}), 500


#////////////////////////////////////////////////////////////////////////////NEW


@app.route('/remove_columns', methods=['POST'])
def remove_columns_route():
    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    # Vérifier si 'columns_to_remove' est dans la requête
    columns_to_remove = request.form.getlist('columns_to_remove')
    if not columns_to_remove:
        return jsonify({'error': 'Au moins une colonne à supprimer est requise'}), 400

    # Enregistrer le fichier téléchargé
    os.makedirs('/app/uploads', exist_ok=True)
    file_path = os.path.join('/app/uploads', file.filename)
    file.save(file_path)
    if not os.path.exists(file_path):
        logging.info("le fichier n'existe pas 803")
        raise FileNotFoundError(f"Le fichier {file_path} n'existe pas.")

    try:
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Suppression'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Suppression',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)
        # Lancer la tâche Celery pour supprimer les colonnes
        task = remove_columns.delay(file_path, columns_to_remove)
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': 'Tâche en cours pour supprimer les colonnes spécifiées',
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500

@app.route('/extract_columns', methods=['POST'])
def extract_columns_route():
    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    # Vérifier si 'columns' est dans la requête
    columns_to_extract = request.form.getlist('columns')
    if not columns_to_extract:
        return jsonify({'error': 'Aucune colonne spécifiée pour extraction'}), 400

    # Enregistrer le fichier téléchargé
    os.makedirs('uploads', exist_ok=True)
    file_path = os.path.join('uploads', file.filename)
    file.save(file_path)

    try:
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Extraction'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Extraction',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)
        # Lancer la tâche Celery pour extraire les colonnes
        task = extract_columns.delay(file_path, columns_to_extract)
        # Ajouter une entrée à la fin avec état "success"
        
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': 'Tâche en cours pour extraire les colonnes spécifiées',
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


# Fonction pour nettoyer les noms de colonnes
def sanitize_column_name(column_name):
    """Nettoie le nom de la colonne en enlevant les caractères spéciaux et les espaces."""
    # Remplacer les espaces par des underscores
    column_name = column_name.replace(' ', '')
    # Enlever les caractères spéciaux en ne gardant que les lettres et chiffres
    column_name = re.sub(r'[^\w]', '', column_name)
    return column_name
 
@app.route('/add_column_with_formula', methods=['POST'])
def add_column_with_formula_route():
    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    # Vérifier si 'new_column_name' et 'formula' sont dans la requête
    new_column_name = request.form.get('new_column_name')
    formula = request.form.get('formula')

    if not new_column_name or not formula:
        return jsonify({'error': 'Le nom de la nouvelle colonne et la formule sont nécessaires'}), 400

    # Nettoyer le nom de la colonne
    new_column_name = sanitize_column_name(new_column_name)

    # Enregistrer le fichier téléchargé
    os.makedirs('uploads', exist_ok=True)
    file_path = os.path.join('uploads', file.filename)
    file.save(file_path)

    try:
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Ajout par formule'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Ajout par formule',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)
        # Lancer la tâche Celery pour ajouter la nouvelle colonne
        task = add_column_with_formula.delay(file_path, new_column_name, formula)
        
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': 'Tâche en cours pour ajouter la nouvelle colonne avec la formule spécifiée',
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


@app.route('/clean_and_validate_data', methods=['POST'])
def clean_and_validate_data_route():
    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    file = request.files['file']
    if not file:
        return jsonify({'error': 'Aucun fichier à uploader'}), 400

    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    validation_rules = request.form.get('validation_rules')
    if not validation_rules:
        return jsonify({'error': 'Aucune règle de validation trouvée dans la requête'}), 400

    validation_rules = json.loads(validation_rules)
    os.makedirs('uploads', exist_ok=True)

    try:
        if file.filename == '':
            return jsonify({'error': 'Nom de fichier invalide'}), 400

        # Sauvegarde du fichier dans un répertoire temporaire avec un nom unique
        save_path = os.path.join('uploads', FileNameGenerator.generate_unique_filename(file.filename))
        file.save(save_path)

        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(save_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(save_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(save_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Validation format'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Validation format',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }

        # Appel de la méthode add_entry
        entry = TableauDeBord.add_entry(entry_data)

        #print(f'Valeur du id ---------: {entry.id}')
        # Lancement de la tâche Celery pour nettoyer et valider les données avec les règles de validation
        task = clean_and_validate_data.delay(save_path, validation_rules)
        
        # Mettre à jour l'entrée avec l'ID de la tâche Celery
        entry.task = task.id
        db.session.commit()

        download_url = f"http://127.0.0.1:5000/download_clean_csv/{task.id}"

        
        # Retournez un message de succès adapté à la vue Vue.js
        return jsonify({
            'message': "Tâche en cours pour nettoyer et valider les données.",
            'task_id': task.id,
            'download_url': download_url
        })
    except Exception as e:
        # Gérez les exceptions et retournez une réponse d'erreur
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500

import logging

logging.basicConfig(level=logging.INFO)

@app.route('/validate_urls', methods=['POST'])
def validate_urls_route():
    logging.info("Démarrage de la route validate_urls_route")

    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        logging.error("Aucun fichier trouvé dans la requête")
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        logging.error("Cookie non trouvé")
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        logging.error("Nom de fichier invalide")
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    # Vérifier si 'url_column' est dans la requête
    url_column = request.form.get('url_column')
    if not url_column:
        logging.error("Aucune colonne URL spécifiée")
        return jsonify({'error': 'Aucune colonne URL spécifiée'}), 400

    # Enregistrer le fichier téléchargé
    os.makedirs('/app/uploads', exist_ok=True)
    file_path = os.path.join('/app/uploads', file.filename)
    
    try:
        file.save(file_path)
        logging.info(f"Fichier enregistré à {file_path}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors de l'enregistrement du fichier : {str(e)}"}), 500

    try:
        
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file_path,dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Validation URL'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Validation URL',
            'nom_du_fichier': file.filename,
            'etat': 'interrupted',
            'task':None,
            'nombre_de_ligne': nombre_de_lignes
        }
        # Appel de la méthode add_entry
        #entry = TableauDeBord(**entry_data)
        entry = TableauDeBord.add_entry(entry_data)

        # Lancer la tâche Celery pour valider les URLs
        task = validate_urls_in_csv.delay(file_path, url_column)
        logging.info(f"Tâche Celery démarrée avec l'ID {task.id}")

        entry.task = task.id
        db.session.commit()

        
        return jsonify({
            'message': 'Tâche en cours pour valider les URLs',
            'task_id': task.id
        })
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Log l'erreur
    logger.error(f"Erreur serveur : {e}")
    # Retourner une réponse HTTP 500
    return jsonify({"error": "Une erreur interne du serveur s'est produite."}), 500

@app.route('/get_entries', methods=['POST'])
def get_entries_route():
    # Vérifiez si le cookie est présent dans la requête
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400

    # Vérifiez si l'état est dans la requête
    etat = request.json.get('etat')
    if etat is None:
        return jsonify({'error': 'État non spécifié'}), 400

    try:
        # Récupérer les entrées directement
        entries, total_count = TableauDeBord.get_entries_by_cookie_and_state(cookie_id, etat)

        # Créer une entrée pour indiquer le succès de la récupération
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'get_entries',
            'nom_du_fichier': 'N/A',  # Aucune valeur de fichier spécifiée
            'etat': 'success',
            'task': None
        }

        # Appel de la méthode add_entry pour enregistrer l'action
        #TableauDeBord.add_entry(entry_data)

        # Retourner les résultats
        return jsonify({
           'entries': [TableauDeBord.to_dict(entry) for entry in entries],  # Utilisez to_dict de TableauDeBord
            'total_count': total_count
        }), 200
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement de la demande : {str(e)}"}), 500

@app.route('/check_task_status/<task_id>', methods=['GET'])
def check_task_status(task_id):
    """Vérifier l'état d'une tâche Celery."""
    task = get_entries_by_cookie_and_state_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'status': 'En attente...'
        }
    elif task.state == 'FAILURE':
        response = {
            'state': task.state,
            'status': str(task.info),  # Ce sera la description de l'exception levée
        }
    else:
        response = {
            'state': task.state,
            'result': task.result
        }
    return jsonify(response)

@app.route('/mouvementpercentage', methods=['POST'])
def mouvement_percentage():
    # Récupérer le paramètre 'etat' de la requête
    etat = request.json.get('etat')

    if not etat:
        return jsonify({"error": "Le paramètre 'etat' est requis."}), 400

    try:
        # Appeler la méthode pour obtenir les pourcentages de mouvements par état
        result = TableauDeBord.get_mouvement_percentage_by_state(etat)

        # Afficher les résultats dans la console
        print("Résultat des pourcentages de mouvements :", result)

        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/mouvementpercentagebydateandstate', methods=['POST'])
def mouvement_percentage_by_date_and_state():
    try:
        # Récupérer les paramètres envoyés depuis le frontend
        etat = request.json.get('etat')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')

        # Valider les paramètres
        if not etat or not start_date or not end_date:
            return jsonify({"error": "Paramètres manquants"}), 400

        # Appeler la méthode de classe pour obtenir les résultats filtrés
        results = TableauDeBord.get_mouvement_percentage_by_state_and_date(etat, start_date, end_date)

        # Si aucun résultat n'est trouvé, retourner une erreur
        if not results:
            return jsonify({"error": "Aucune donnée trouvée pour cette plage de dates et cet état"}), 404

        # Retourner les résultats sous forme de JSON
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/mouvementpercentagebydateandstatenope', methods=['POST'])
def mouvement_percentage_by_date_and_state_nope():
    try:
        # Récupérer les paramètres envoyés depuis le frontend
        etat = request.json.get('etat')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')

        # Convertir les dates reçues en objets datetime
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Convertir les données en DataFrame pour un traitement facile
        df = pd.DataFrame(data)

        # Convertir la colonne 'mouvement' en datetime pour le filtrage par date
        df['mouvement'] = pd.to_datetime(df['mouvement'])

        # Filtrer par type de statistique (etat)
        filtered_df = df[df['etat'] == etat]

        # Filtrer par dates si elles sont fournies
        if start_date:
            filtered_df = filtered_df[filtered_df['mouvement'] >= start_date]
        if end_date:
            filtered_df = filtered_df[filtered_df['mouvement'] <= end_date]

        # Si aucune donnée n'est trouvée, retourner une erreur
        if filtered_df.empty:
            return jsonify({"error": "Aucune donnée trouvée pour cette plage de dates et ce type d'état"}), 404

        # Convertir le DataFrame filtré en une liste de dictionnaires
        result = filtered_df.to_dict(orient='records')

        # Retourner les données filtrées
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Configurer la journalisation
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/mouvementstatistics', methods=['POST'])
def mouvement_statistics():
    try:
        id_cookie = request.cookies.get('cookie_id')
        time_interval = request.json.get('interval')

        logging.info(f"Requête reçue: id_cookie={id_cookie}, time_interval={time_interval}")

        if not id_cookie:
            return jsonify({'error': 'id_cookie is required'}), 400

        stats = TableauDeBord.get_mouvement_statistics(id_cookie=id_cookie, time_interval=time_interval)
        logging.info(f"Statistiques obtenues: {stats}")

        return jsonify({'statistics': stats}), 200

    except Exception as e:
        logging.error(f"Erreur: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/aggregated_data', methods=['GET'])
def aggregated_data():
    """Route pour récupérer des données agrégées ou le poids total par mois."""
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    data_type = request.args.get('type', type=str)  # 'aggregated' ou 'total_weight'

    if not month or not year:
        return jsonify({"error": "Month and year are required parameters."}), 400

    try:
        result=None
        total=None
        if data_type == 'aggregated':
            result = Metadata.get_aggregated_data_by_month(month, year)
            total = Metadata.get_total_weight_by_month(month, year)  # Remplacez par votre méthode
        else:
            return jsonify({"error": "Invalid type parameter. Use 'aggregated' or 'total_weight'."}), 400

         # Retournez à la fois result et total dans la réponse JSON
        return jsonify({
            "result": result,
            "total_weight": total
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/detect_outliers', methods=['POST'])
def detect_outliers_route():
    logging.info("Démarrage de la route detect_outliers_route")

    # Vérifiez si 'file' est dans la requête
    if 'file' not in request.files:
        logging.error("Aucun fichier trouvé dans la requête")
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    cookie_id = request.cookies.get('cookie_id')
    logging.info(f"Le cookie_id {cookie_id}")
    if not cookie_id:
        logging.error("Cookie non trouvé")
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        logging.error("Nom de fichier invalide")
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    # Vérifier si 'key_column' est dans la requête
    key_column = request.form.get('column')
    if not key_column:
        logging.error("Aucune colonne clé spécifiée")
        return jsonify({'error': 'Aucune colonne clé spécifiée'}), 400

    # Vérifier si 'age_check' est dans la requête pour déterminer le type de détection
    age_check = request.form.get('age_check', 'false').lower() == 'true'
    #detect_non_numeric = request.form.get('detect_non_numeric', 'false').lower() == 'true'

    # Enregistrer le fichier téléchargé
    os.makedirs('uploads', exist_ok=True)
    file_path = os.path.join('uploads', file.filename)
    try:
        file.save(file_path)
        logging.info(f"Fichier enregistré à {file_path}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors de l'enregistrement du fichier : {str(e)}"}), 500

    try:
        # Obtenir le poids du fichier en Mo
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        # Lire le fichier CSV avec Dask pour obtenir le nombre de lignes et de colonnes
        # df = dd.read_csv(file_path, dtype='object')
        # nombre_de_lignes = df.shape[0].compute()  # Obtenir le nombre de lignes
        # nombre_de_colonnes = df.shape[1]  # Obtenir le nombre de colonnes
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file_path)
        # Préparer les données pour la table metadata
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,  # À remplir après le traitement si nécessaire
            'poids': poids,
            'unite': 'Mo',  # Unité en Mo
            'action': 'Détection des outliers'  # Action
        }

        # Ajoutez l'entrée à la table metadata
        metadata = Metadata(**metadata_entry)  # Créez un objet Metadata
        db.session.add(metadata)  # Ajoutez à la session
        db.session.commit()  # Validez la transaction

        # Ajouter une entrée à la fin avec état "success"
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Détection des outliers',
            'nom_du_fichier': file.filename,
            'etat': 'succes',
            'task': None,
            'nombre_de_ligne': nombre_de_lignes
        }
        # Appel de la méthode add_entry
        entry = TableauDeBord.add_entry(entry_data)

        # Lancer la tâche Celery pour détecter les outliers
        if age_check:
            logging.info("Traitement dans age_check")
            task = detect_age_outliers.delay(file_path, key_column)  # Appel pour l'âge
        else:
            logging.info("Traitement non age_check")
            task = detect_outliers.delay(file_path, key_column)  # Appel général pour d'autres colonnes

        logging.info(f"Tâche Celery démarrée avec l'ID {task.id}")

        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': 'Tâche de détection des outliers lancée',
            'task_id': task.id
        })
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


@app.route('/detect_non_numeric', methods=['POST'])
def detect_non_numeric_route():
    logging.info("Démarrage de la route detect_non_numeric_route")

    if 'file' not in request.files:
        logging.error("Aucun fichier trouvé dans la requête")
        return jsonify({'error': 'Aucun fichier trouvé dans la requête'}), 400

    cookie_id = request.cookies.get('cookie_id')
    if not cookie_id:
        logging.error("Cookie non trouvé")
        return jsonify({'error': 'Cookie non trouvé'}), 400

    file = request.files['file']
    if file.filename == '':
        logging.error("Nom de fichier invalide")
        return jsonify({'error': 'Nom de fichier invalide'}), 400

    key_column = request.form.get('column')
    if not key_column:
        logging.error("Aucune colonne clé spécifiée")
        return jsonify({'error': 'Aucune colonne clé spécifiée'}), 400

    # Enregistrer le fichier téléchargé
    os.makedirs('uploads', exist_ok=True)
    file_path = os.path.join('uploads', file.filename)
    try:
        file.save(file_path)
        logging.info(f"Fichier enregistré à {file_path}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors de l'enregistrement du fichier : {str(e)}"}), 500

    try:
        poids = os.path.getsize(file_path) / (1024 * 1024)  # Convertir en Mo
        df = dd.read_csv(file_path, dtype='object')
        nombre_de_lignes = df.shape[0].compute()
        nombre_de_colonnes = df.shape[1]

        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,
            'poids': poids,
            'unite': 'Mo',
            'action': 'Détection des non numériques'
        }

        metadata = Metadata(**metadata_entry)
        db.session.add(metadata)
        db.session.commit()

        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Détection des non numériques',
            'nom_du_fichier': file.filename,
            'etat': 'succes',
            'task': None,
            'nombre_de_ligne': nombre_de_lignes
        }
        entry = TableauDeBord.add_entry(entry_data)

        logging.info("Traitement pour les valeurs non numériques")
        task = detect_non_numeric_values.delay(file_path, key_column)
        
        logging.info(f"Tâche Celery démarrée avec l'ID {task.id}")

        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': 'Tâche de détection des non numériques lancée',
            'task_id': task.id,
            'metadata_id': metadata.id  # Inclure l'ID de la métadonnée dans la réponse
        })
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier : {str(e)}")
        return jsonify({'error': f"Erreur lors du traitement du fichier : {str(e)}"}), 500


@app.route('/task_result_outlier/<task_id>', methods=['GET'])
def get_task_result(task_id):
    task = detect_outliers.AsyncResult(task_id)
    if task.state == 'PENDING':
        return jsonify({'state': task.state}), 202
    elif task.state != 'FAILURE':
        return jsonify({'state': task.state, 'result': task.result}), 200
    else:
        return jsonify({'state': task.state, 'error': str(task.info)}), 500


@app.route('/task_result_non_numeric/<task_id>', methods=['GET'])
def task_result_non_numeric(task_id):
    task_result = AsyncResult(task_id)
    
    if task_result.state == 'PENDING':
        # Tâche en attente
        response = {
            'state': task_result.state,
            'result': None
        }
    elif task_result.state != 'FAILURE':
        # Tâche réussie
        response = {
            'state': task_result.state,
            'result': task_result.result  # Ce sera le rapport que vous avez retourné
        }
    else:
        # Tâche échouée
        response = {
            'state': task_result.state,
            'result': str(task_result.info)  # Récupérer les détails de l'erreur
        }

    return jsonify(response)

def count_columns(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline()
        nombre_de_colonnes = len(first_line.split(','))
    return nombre_de_colonnes

def count_lines(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        nombre_de_lignes = sum(1 for line in file)
    return nombre_de_lignes

def count_rows_columns(file_path):
    nombre_de_lignes = count_lines(file_path)
    nombre_de_colonnes = count_columns(file_path)
    return nombre_de_lignes, nombre_de_colonnes



@app.route('/compare_two_csv', methods=['POST'])
def compare_two_csv_route():
    # Vérifiez la présence du cookie
    cookie_id = request.cookies.get('cookie_id')
    if cookie_id is None:
        return jsonify({'error': 'Cookie non trouvé'}), 400
    
    # Assurez-vous que le répertoire de téléchargement existe
    #os.makedirs('uploads', exist_ok=True)

    # Vérifiez les fichiers dans la requête
    # if 'files' not in request.files:
    #     return jsonify({'error': 'Both files are required'}), 400

    # files = request.files.getlist('files')
    
    # if len(files) != 2:
    #     return jsonify({'error': 'Exactly two files are required'}), 400

    file1 = request.files['file1']
    file2 = request.files['file2']
    
    if file1.filename == '' or file2.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if not (file1.filename.endswith('.csv') and file2.filename.endswith('.csv')):
        return jsonify({'error': 'Only CSV files are allowed'}), 400

    
    try:
        # Sauvegarder les fichiers dans le répertoire de téléchargement
        file1_path = os.path.join('/app/uploads', FileNameGenerator.generate_unique_filename(file1.filename))
        file2_path = os.path.join('/app/uploads', FileNameGenerator.generate_unique_filename(file2.filename))
        
        file1.save(file1_path)
        file2.save(file2_path)
        if not os.path.exists(file1_path):
            logging.info("le fichier n'existe pas 1083")
            raise FileNotFoundError(f"Le fichier {file1_path} n'existe pas.")
        # Calcul des métadonnées pour le premier fichier
        poids = os.path.getsize(file1_path) / (1024 * 1024)  # Convertir en Mo
        nombre_de_lignes, nombre_de_colonnes = count_rows_columns(file1_path)
        
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file1.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_lignes,
            'statistiques_descriptives': None,
            'poids': poids,
            'unite': 'Mo',
            'action': 'Comparaison'
        }
        metadata = Metadata(**metadata_entry)
        db.session.add(metadata)
        db.session.commit()

        # Calcul des métadonnées pour le deuxième fichier
        poids = os.path.getsize(file2_path) / (1024 * 1024)
        nombre_de_ligne, nombre_de_colonnes = count_rows_columns(file2_path)
        
        metadata_entry = {
            'id_cookie': cookie_id,
            'nom_du_fichier': file2.filename,
            'nombre_de_colonnes': nombre_de_colonnes,
            'nombre_de_lignes': nombre_de_ligne,
            'statistiques_descriptives': None,
            'poids': poids,
            'unite': 'Mo',
            'action': 'Comparaison'
        }
        name=file1.filename+'/'+file2.filename
        nbr=nombre_de_ligne+nombre_de_lignes
        metadata = Metadata(**metadata_entry)
        db.session.add(metadata)
        db.session.commit()

        # Ajouter une entrée au tableau de bord
        entry_data = {
            'id_cookie': cookie_id,
            'mouvement': 'Comparaison',
            'nom_du_fichier': name,
            'etat': 'en cours',
            'task': None,
            'nombre_de_ligne': nbr
        }
        entry = TableauDeBord.add_entry(entry_data)

        # Lancer la tâche Celery pour nettoyer les fichiers CSV
        task = compare_two_csv.apply_async((file1_path, file2_path))
        entry.task = task.id
        db.session.commit()

        return jsonify({
            'message': f"Tâche en cours pour comparer les fichiers CSV : {file1_path} et {file2_path}",
            'task_id': task.id
        })
    except Exception as e:
        return jsonify({'error': f"Erreur lors du traitement des fichiers : {str(e)}"}), 500


@app.route('/get_file_content/<task_id>', methods=['GET'])
def get_file_content(task_id):
    try:
        logging.info(f"Task ID in get file content: {task_id}")

        # Récupérer l'état de la tâche à partir de Celery
        result = celery.AsyncResult(task_id)

        # Vérifier si la tâche est prête et si elle n'a pas échoué
        if result.ready() and not result.failed():
            # Récupérer les chemins des fichiers nettoyés
            cleaned_file_paths = result.get()  # Supposons que cela retourne une liste de chemins de fichiers

            if not isinstance(cleaned_file_paths, list) or len(cleaned_file_paths) < 2:
                return jsonify({'error': 'Les fichiers nettoyés non trouvés'}), 404

            # Lire le contenu des fichiers
            contents = []
            for file_path in cleaned_file_paths:
                if not os.path.exists(file_path):
                    return jsonify({'error': f'Fichier non trouvé: {file_path}'}), 404
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    contents.append(f.read())

            # Renvoyer le contenu des deux fichiers
            return jsonify({
                'file1_content': contents[0],
                'file2_content': contents[1]
            })

        else:
            return jsonify({'error': 'La tâche n\'est pas encore prête ou a échoué'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_file_content/<path:file_path>', methods=['GET'])
def get_file_content_old(file_path):
    try:
        logging.info(f"File path in get file content {file_path}")
        # Assurez-vous que le fichier existe
        if not os.path.exists(file_path):
            return jsonify({'error': 'Fichier non trouvé'}), 404

        # Envoyer le contenu du fichier comme réponse
        return send_file(file_path)  # Envoie le fichier pour téléchargement
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
