# celery_config.py
from celery import Celery

def make_celery(app):
   
    celery = Celery(
        app.import_name,
        broker=app.config['broker_url'],  # URL du broker
        backend=app.config['result_backend']  # Backend des résultats
    )
    
    # Mettre à jour la configuration de Celery avec les paramètres de l'application Flask
    celery.conf.update(app.config)

    # Configuration additionnelle pour Celery
    celery.conf.update({
        'timezone': 'Asia/Gaza',             # Fuseau horaire pour les tâches planifiées
        'result_expires': 360,               # Temps d'expiration des résultats
        'broker_heartbeat': 49,              # Intervalle de heartbeat du broker en secondes
        'worker_heartbeat': 49,              # Intervalle de heartbeat des workers en secondes
        'broker_connection_retry_on_startup': True,  # Activation des retries lors du démarrage
        
        # Options de transport du broker
        'broker_transport_options': {
            'visibility_timeout': 3600,  # Temps d'attente maximum avant de déclarer une tâche comme échouée
            'retry_policy': {             # Politique de retry des connexions
                'interval_start': 0,      # Temps d'attente initial avant la première retry
                'interval_step': 0.2,     # Incrément du délai entre les retries
                'interval_max': 0.2,      # Délai maximum avant retry
                'max_retries': 100        # Nombre maximal de retries
            },
            'socket_timeout': 30          # Délai avant expiration de la connexion socket
        }
         # Enable debug level logging
        'worker_log_level': 'DEBUG',
        'worker_redirect_stdouts_level': 'DEBUG'
    })
    
    return celery
