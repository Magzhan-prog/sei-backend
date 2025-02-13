from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from constants import USER, PASSWORD, HOST, PORT, DB

# Замените параметры подключения на свои
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

# Создаем движок подключения к базе данных
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# SQLAlchemy-модель для хранения данных
class UserData(Base):
    __tablename__ = "user_data"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)  # Берется из cookie
    p_index_id = Column(Integer, nullable=False)
    p_period_id = Column(Integer, nullable=False)
    p_terms = Column(String, nullable=False)
    p_term_id = Column(Integer, nullable=False)
    p_dicIds = Column(String, nullable=False)   # Храним как строку
    idx = Column(Integer, nullable=False)

# Создаем таблицы, если они ещё не существуют
Base.metadata.create_all(bind=engine)