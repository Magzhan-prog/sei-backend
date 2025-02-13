from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from constants import USER, PASSWORD, HOST, DB

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DB}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class IndexTreeData(Base):
    __tablename__ = "index_tree_data"

    id = Column(String, primary_key=True)
    p_index_id = Column(Integer, primary_key=True)  # Идентификатор показателя
    p_period_id = Column(Integer, primary_key=True)  # Идентификатор типа периода
    p_terms = Column(String, primary_key=True)  # Список элементов для выборки
    p_term_id = Column(Integer, primary_key=True)  # Главный элемент для детализации
    p_dicIds = Column(String, primary_key=True)  # Список справочников
    idx = Column(Integer, primary_key=True)  # Индекс разрезности
    p_parent_id = Column(String, primary_key=True)  # Идентификатор родительского элемента
    response_data = Column(JSON)  # Ответ от API в формате JSON

Base.metadata.create_all(bind=engine)