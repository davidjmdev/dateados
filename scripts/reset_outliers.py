
import logging
from sqlalchemy import create_engine, text
from db.connection import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_outliers():
    engine = create_engine(DATABASE_URL)
    
    tables_to_clear = [
        "outliers_player_season_state",
        "outliers_player",
        "outliers_player_trends",
        "outliers_streaks",
        "outliers_streak_all_time_records"
    ]
    
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            # Desactivar constraints temporalmente si es necesario (Postgres)
            conn.execute(text("SET session_replication_role = 'replica';"))
            
            for table in tables_to_clear:
                logger.info(f"Vaciando tabla {table}...")
                conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
            
            conn.execute(text("SET session_replication_role = 'origin';"))
            transaction.commit()
            logger.info("✅ Sistema de outliers reseteado con éxito.")
        except Exception as e:
            transaction.rollback()
            logger.error(f"❌ Error al resetear outliers: {e}")
            raise

if __name__ == "__main__":
    reset_outliers()
