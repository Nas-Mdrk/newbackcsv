from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy import case,func
from db_manager import get_session, close_session
from extensions import db
import uuid
import os
from datetime import datetime,date,timedelta
import logging

class User(db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    @classmethod
    def get_all_users(cls):
        """Retourne tous les utilisateurs."""
        try:
            print('haha1')
            users = cls.query.all()
            print('haha2')
            return users
        except Exception as e:
            print(f"Erreur lors de la récupération des utilisateurs : {e}")
            raise

     
    @classmethod
    def add_user(cls, user_data):
        """Ajoute un nouvel utilisateur."""
        try:
            new_user = cls(**user_data)
            db.session.add(new_user)
            db.session.commit()
            print(f"Utilisateur {new_user.id} ajouté.")
            return new_user
        except IntegrityError as e:
            db.session.rollback()
            print(f"Erreur lors de l'ajout de l'utilisateur : {e}")
            raise
        except Exception as e:
            db.session.rollback()
            print(f"Erreur inattendue : {e}")
            raise

    @classmethod
    def update_user(cls, user_id, user_data):
        """Met à jour les informations d'un utilisateur."""
        try:
            user = cls.query.get(user_id)
            if user:
                for key, value in user_data.items():
                    setattr(user, key, value)
                db.session.commit()
                print(f"Utilisateur {user_id} mis à jour.")
                return user
            else:
                print(f"Utilisateur {user_id} non trouvé.")
                return None
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la mise à jour de l'utilisateur : {e}")
            raise

    @classmethod
    def delete_user(cls, user_id):
        """Supprime un utilisateur."""
        try:
            user = cls.query.get(user_id)
            if user:
                db.session.delete(user)
                db.session.commit()
                print(f"Utilisateur {user_id} supprimé.")
                return True
            else:
                print(f"Utilisateur {user_id} non trouvé.")
                return False
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la suppression de l'utilisateur : {e}")
            raise

    @classmethod
    def to_dict(cls, user):
        """Convertit un utilisateur en dictionnaire."""
        return {'id': user.id, 'username': user.username, 'email': user.email}


# new_user = User.add_user({
#     'username': 'janedoe',
#     'email': 'janedoe@example.com',
#     'password': 'mypassword'
# })
# updated_user = User.update_user(1, {
#     'email': 'jane.doe@newdomain.com',
#     'password': 'newpassword'
# })

class FileNameGenerator:
    @staticmethod
    def generate_unique_filename(original_filename, extension=None):
        """Génère un nom de fichier unique en utilisant un UUID"""
        unique_id = uuid.uuid4().hex
        base, ext = os.path.splitext(original_filename)

        # Déterminer l'extension à utiliser
        if extension:
            ext = '.' + extension
        else:
            ext = '.csv'  # Ajouter une extension CSV par défaut si aucune extension n'est fournie

        # Créer le nouveau nom de fichier unique
        new_filename = f"{base}_{unique_id}{ext}"
        return new_filename
    
class TableauDeBord(db.Model):
    __tablename__ = 'tableaudebord'

    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    id_cookie = db.Column(db.String(250), nullable=False)
    mouvement = db.Column(db.String(50), nullable=False)
    jour = db.Column(db.Date, default=date.today)
    nom_du_fichier = db.Column(db.String(100), nullable=False)
    etat = db.Column(db.String(50), nullable=False)
    task = db.Column(db.String(100), nullable=True)  # Nouvelle colonne ajoutée précédemment
    nombre_de_ligne = db.Column(db.Integer, nullable=False, default=0)  # Nouvelle colonne
    
    @classmethod
    def get_all_entries(cls):
        """Retourne toutes les entrées du tableau de bord."""
        try:
            entries = cls.query.all()
            return entries
        except Exception as e:
            print(f"Erreur lors de la récupération des entrées : {e}")
            raise

    @classmethod
    def add_entry(cls, entry_data):
        """Ajoute une nouvelle entrée."""
        try:
            if 'nombre_de_ligne' not in entry_data:
                entry_data['nombre_de_ligne'] = 0  # Valeur par défaut

            new_entry = cls(**entry_data)
            db.session.add(new_entry)
            db.session.commit()
            print(f"Entrée {new_entry.id_cookie} ajoutée.")
            return new_entry
        except IntegrityError as e:
            db.session.rollback()
            print(f"Erreur d'intégrité lors de l'ajout de l'entrée : {e}")
            raise
        except Exception as e:
            db.session.rollback()
            print(f"Erreur inattendue lors de l'ajout de l'entrée : {e}")
            raise


    @classmethod
    def add_entry_directly(cls, id_cookie, mouvement, nom_du_fichier, etat, task=None, nombre_de_ligne=0):
        """Ajoute une nouvelle entrée avec un état, une tâche, et un nombre de lignes spécifiés."""
        try:
            new_entry = cls(
                id_cookie=id_cookie,
                mouvement=mouvement,
                jour=date.today(),
                nom_du_fichier=nom_du_fichier,
                etat=etat,
                task=task,
                nombre_de_ligne=nombre_de_ligne  # Ajouter la nouvelle colonne
            )
            db.session.add(new_entry)
            db.session.commit()
            print(f"Entrée {new_entry.id_cookie} ajoutée avec l'état '{etat}', la tâche '{task}', et {nombre_de_ligne} lignes.")
            return new_entry
        except IntegrityError as e:
            db.session.rollback()
            print(f"Erreur d'intégrité lors de l'ajout de l'entrée : {e}")
            raise
        except Exception as e:
            db.session.rollback()
            print(f"Erreur inattendue lors de l'ajout de l'entrée : {e}")
            raise


    @classmethod
    def update_entry(cls, id_cookie, entry_data):
        """Met à jour les informations d'une entrée."""
        try:
            entry = cls.query.filter_by(id_cookie=id_cookie).first()
            if entry:
                for key, value in entry_data.items():
                    setattr(entry, key, value)
                db.session.commit()
                print(f"Entrée {id_cookie} mise à jour.")
                return entry
            else:
                print(f"Entrée {id_cookie} non trouvée.")
                return None
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la mise à jour de l'entrée : {e}")
            raise

    @classmethod
    def delete_entry(cls, id_cookie):
        """Supprime une entrée."""
        try:
            entry = cls.query.filter_by(id_cookie=id_cookie).first()
            if entry:
                db.session.delete(entry)
                db.session.commit()
                print(f"Entrée {id_cookie} supprimée.")
                return True
            else:
                print(f"Entrée {id_cookie} non trouvée.")
                return False
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la suppression de l'entrée : {e}")
            raise

    @classmethod
    def to_dict(cls, entry):
        """Convertit une entrée en dictionnaire."""
        return {
            'id_cookie': entry.id_cookie,
            'mouvement': entry.mouvement,
            'jour': entry.jour.strftime('%d-%m-%Y'),
            'nom_du_fichier': entry.nom_du_fichier,
            'etat': entry.etat,
            'task': entry.task,  # Inclure la nouvelle colonne 'task'
            'nombre_de_ligne': entry.nombre_de_ligne  # Ajouter la nouvelle colonne 'nombre_de_ligne'
        }

        
    @classmethod
    def get_entries_by_cookie(cls, id_cookie):
        """Retourne toutes les entrées associées à un id_cookie donné."""
        try:
            entries = cls.query.filter_by(id_cookie=id_cookie).all()
            return entries
        except Exception as e:
            print(f"Erreur lors de la récupération des entrées pour id_cookie {id_cookie} : {e}")
            raise

    @classmethod
    def get_entries_by_cookie_and_state(cls, id_cookie, etat):
        """Retourne toutes les entrées associées à un id_cookie et un état donnés ainsi que le nombre total de lignes."""
        try:
            entries = cls.query.filter_by(id_cookie=id_cookie, etat=etat).all()
            total_count = len(entries)  # Compter le nombre total d'entrées
            return entries, total_count
        except Exception as e:
            print(f"Erreur lors de la récupération des entrées pour id_cookie {id_cookie} et état {etat} : {e}")
            raise


    @classmethod
    def get_mouvement_percentage(cls):
        """Retourne les mouvements et leur pourcentage par rapport au nombre total d'apparitions."""
        try:
            # Total des mouvements
            total_movements = db.session.query(func.count(cls.mouvement)).scalar()

            # Compte le nombre d'apparitions de chaque mouvement
            movements_count = db.session.query(
                cls.mouvement,
                func.count(cls.mouvement).label('count')
            ).group_by(cls.mouvement).all()

            # Calcul du pourcentage
            movements_percentage = [
                {
                    'mouvement': mouvement,
                    'count': count,
                    'percentage': (count / total_movements) * 100
                }
            for mouvement, count in movements_count
            ]

            return movements_percentage

        except Exception as e:
            print(f"Erreur lors du calcul des mouvements et des pourcentages : {e}")
            raise
    
    
    @classmethod
    def get_mouvement_percentage_by_state(cls, etat):
        """Retourne les mouvements et leur pourcentage pour les entrées avec un état spécifié."""
        try:
            # Filtrer les entrées avec l'état spécifié
            query = cls.query.filter_by(etat=etat)

            # Total des mouvements avec l'état spécifié
            total_movements = query.count()

            # Compte le nombre d'apparitions de chaque mouvement avec l'état spécifié
            movements_count = db.session.query(
                cls.mouvement,
                func.count(cls.mouvement).label('count')
            ).filter_by(etat=etat).group_by(cls.mouvement).all()

            # Calcul du pourcentage
            movements_percentage = [
                {
                    'mouvement': mouvement,
                    'count': count,
                    'percentage': (count / total_movements) * 100
                }
            for mouvement, count in movements_count
            ]

            return movements_percentage

        except Exception as e:
            print(f"Erreur lors du calcul des mouvements et des pourcentages pour l'état '{etat}' : {e}")
            raise


    @classmethod
    def get_mouvement_percentage_by_state_and_date(cls, etat, start_date, end_date):
        """Retourne les mouvements et leur pourcentage pour les entrées avec un état spécifié, entre deux dates."""
        try:
            # Filtrer les entrées avec l'état spécifié et entre les deux dates
            query = cls.query.filter(
                cls.etat == etat,
                cls.jour >= start_date,
                cls.jour <= end_date
            )

            # Total des mouvements avec l'état spécifié et dans l'intervalle de dates
            total_movements = query.count()

            # Si aucun mouvement n'est trouvé, retourner une liste vide pour éviter la division par zéro
            if total_movements == 0:
                return []

            # Compter le nombre d'apparitions de chaque mouvement avec l'état spécifié et entre les deux dates
            movements_count = db.session.query(
                cls.mouvement,
                func.count(cls.mouvement).label('count')
            ).filter(
                cls.etat == etat,
                cls.jour >= start_date,
                cls.jour <= end_date
            ).group_by(cls.mouvement).all()

            # Calcul du pourcentage
            movements_percentage = [
                {
                    'mouvement': mouvement,
                    'count': count,
                    'percentage': (count / total_movements) * 100
                }
                for mouvement, count in movements_count
            ]

            return movements_percentage

        except Exception as e:
            print(f"Erreur lors du calcul des mouvements et des pourcentages pour l'état '{etat}' entre les dates '{start_date}' et '{end_date}' : {e}")
            raise

    @classmethod
    def get_mouvement_statistics(cls, id_cookie, time_interval='day'):
        try:
            query = db.session.query(
                cls.mouvement,
                func.count(cls.id).label('total_utilisation'),
                func.sum(case(
                    (cls.etat == 'succes', 1),
                    else_=0
                )).label('total_succes'),
                func.sum(case(
                    (cls.etat == 'fail', 1),
                    else_=0
                )).label('total_fail'),
                func.date_trunc(time_interval, cls.jour).label('periode')
            ).filter_by(id_cookie=id_cookie).group_by(
                cls.mouvement,
                func.date_trunc(time_interval, cls.jour)
            ).order_by('periode')

            results = query.all()
            logging.debug(f"Résultats de la requête: {results}")

            statistics = []
            for mouvement, total_utilisation, total_succes, total_fail, periode in results:
                statistics.append({
                    'mouvement': mouvement,
                    'total_utilisation': total_utilisation,
                    'total_succes': total_succes,
                    'total_fail': total_fail,
                    'periode': periode.strftime('%d-%m-%Y')
                })

            return statistics

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des statistiques: {e}")
            raise


class Metadata(db.Model):
    __tablename__ = 'metadatas'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    id_cookie = db.Column(db.String(50), nullable=False)
    nom_du_fichier = db.Column(db.String(100), nullable=False)
    nombre_de_colonnes = db.Column(db.Integer, nullable=False)
    nombre_de_lignes = db.Column(db.Integer, nullable=False)
    statistiques_descriptives = db.Column(db.Text, nullable=True)
    poids = db.Column(db.Float, nullable=False)
    unite = db.Column(db.String(20), nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    action = db.Column(db.String(50), nullable=False)  # Nouvelle colonne action

    @classmethod
    def get_all_entries(cls):
        """Retourne toutes les entrées de la table metadata."""
        try:
            entries = cls.query.all()
            return entries
        except Exception as e:
            print(f"Erreur lors de la récupération des entrées : {e}")
            raise

    @classmethod
    def add_entry(cls, entry_data):
        """Ajoute une nouvelle entrée dans la table metadata."""
        try:
            new_entry = cls(**entry_data)
            db.session.add(new_entry)
            db.session.commit()
            print(f"Entrée pour le fichier {new_entry.nom_du_fichier} ajoutée.")
            return new_entry
        except IntegrityError as e:
            db.session.rollback()
            print(f"Erreur d'intégrité lors de l'ajout de l'entrée : {e}")
            raise
        except Exception as e:
            db.session.rollback()
            print(f"Erreur inattendue lors de l'ajout de l'entrée : {e}")
            raise

    @classmethod
    def update_entry(cls, id, entry_data):
        """Met à jour les informations d'une entrée dans la table metadata."""
        try:
            entry = cls.query.get(id)
            if entry:
                for key, value in entry_data.items():
                    setattr(entry, key, value)
                db.session.commit()
                print(f"Entrée avec ID {id} mise à jour.")
                return entry
            else:
                print(f"Entrée avec ID {id} non trouvée.")
                return None
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la mise à jour de l'entrée : {e}")
            raise

    @classmethod
    def delete_entry(cls, id):
        """Supprime une entrée de la table metadata."""
        try:
            entry = cls.query.get(id)
            if entry:
                db.session.delete(entry)
                db.session.commit()
                print(f"Entrée avec ID {id} supprimée.")
                return True
            else:
                print(f"Entrée avec ID {id} non trouvée.")
                return False
        except Exception as e:
            db.session.rollback()
            print(f"Erreur lors de la suppression de l'entrée : {e}")
            raise

    @classmethod
    def to_dict(cls, entry):
        """Convertit une entrée en dictionnaire."""
        return {
            'id': entry.id,
            'id_cookie': entry.id_cookie,
            'nom_du_fichier': entry.nom_du_fichier,
            'nombre_de_colonnes': entry.nombre_de_colonnes,
            'nombre_de_lignes': entry.nombre_de_lignes,
            'statistiques_descriptives': entry.statistiques_descriptives,
            'poids': entry.poids,
            'unite': entry.unite,
            'date': entry.date.strftime('%Y-%m-%d %H:%M:%S'),
            'action': entry.action  # Inclure la colonne action
        }

    @classmethod
    def get_total_weight_by_month(cls, month, year):
        """Retourne le poids total des données pour un mois donné."""
        try:
            total_weight = db.session.query(db.func.sum(cls.poids)).filter(
                db.extract('month', cls.date) == month,
                db.extract('year', cls.date) == year
            ).scalar()
            return total_weight if total_weight is not None else 0
        except Exception as e:
            print(f"Erreur lors de la récupération du poids total par mois : {e}")
            raise
    
    @classmethod
    def get_aggregated_data_by_month(cls, month, year):
        """Retourne le poids total, le nombre total de lignes, et les actions par action et id_cookie pour un mois donné."""
        try:
            results = db.session.query(
                cls.id_cookie,
                cls.action,
                func.sum(cls.poids).label('total_poids'),
                func.sum(cls.nombre_de_lignes).label('total_lignes')
            ).filter(
                db.extract('month', cls.date) == month,
                db.extract('year', cls.date) == year
            ).group_by(cls.id_cookie, cls.action).all()

            # Formater les résultats en dictionnaire
            aggregated_data = [
                {
                    'id_cookie': result.id_cookie,
                    'action': result.action,
                    'total_poids': result.total_poids,
                    'total_lignes': result.total_lignes
                }
                for result in results
            ]
            return aggregated_data
        except Exception as e:
            print(f"Erreur lors de la récupération des données agrégées : {e}")
            raise