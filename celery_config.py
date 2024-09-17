# celery_config.py
from celery import Celery

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )
    #celery.conf.update(app.config)
    # Mettre à jour la configuration de Celery avec les paramètres de l'application
    celery.conf.update({
        'timezone': 'Asia/Gaza',
        'broker_url': app.config['CELERY_BROKER_URL'],   # URL du broker
        'result_backend': app.config['CELERY_RESULT_BACKEND'],  # Backend des résultats
        'result_expires': 360,
        'broker_heartbeat': 49,   # Augmentez si nécessaire
        'worker_heartbeat': 49,   # Augmentez si nécessaire
        #'broker_heartbeat': 20,   Intervalle de heartbeat du broker en secondes
        #'worker_heartbeat': 20,   Intervalle de heartbeat des workers en secondes
    })
    return celery
