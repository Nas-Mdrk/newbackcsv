# Utilisez une image Python comme base
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers de requirements
COPY requirements.txt requirements.txt

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application dans le conteneur
COPY . .

# Exposer le port sur lequel Flask sera en cours d'exécution
EXPOSE 5000

# Définir la commande d'entrée pour lancer Flask et Celery
#CMD ["sh", "-c", "celery -A app.celery worker --loglevel=info & flask run --host=0.0.0.0"]

# Définir la commande d'entrée pour lancer Flask et Celery
CMD ["sh", "-c", "celery -A app.celery worker --loglevel=info & celery -A app.celery beat --loglevel=info & flask run --host=0.0.0.0"]
