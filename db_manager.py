from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URI = 'postgresql://postgres:postgres@host.docker.internal/csv'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

def get_session():
    """Retourne une nouvelle session"""
    return Session()

def close_session(session):
    """Ferme une session"""
    session.close()
