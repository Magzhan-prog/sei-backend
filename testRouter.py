from fastapi import APIRouter, Depends
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy.orm import Session
from constants import USER, PASSWORD, HOST, DB
from sqlalchemy import select
from sqlalchemy import and_

router = APIRouter()

# Настройка базы данных
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class IndexTreeData(Base):
    __tablename__ = "index_tree_data"

    id = Column(String, primary_key=True)
    p_index_id = Column(Integer, primary_key=True)
    p_period_id = Column(Integer, primary_key=True)
    p_terms = Column(String, primary_key=True)
    p_term_id = Column(Integer, primary_key=True)
    p_dicIds = Column(String, primary_key=True)
    idx = Column(Integer, primary_key=True)
    p_parent_id = Column(String, primary_key=True)
    response_data = Column(JSON)

# Функция для получения сессии БД в FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db  # Передаём объект сессии
    finally:
        db.close()  # Закрываем после использования

def build_tree(items, parent_id=''):
    tree = []
    item_dict = {item.id: item for item in items}  # Словарь для быстрого доступа по id

    for item in items:
        if item.p_parent_id == parent_id:
            node = {
                "id": item.id,
                "response_data": item.response_data,
                "children": build_tree(items, item.id)
            }
            tree.append(node)
    
    return tree

@router.get(
    "/test_router"
)
async def testRouter(db: Session = Depends(get_db)):

    query = select(IndexTreeData).where(
        and_(
            IndexTreeData.p_index_id == 18789901,
            IndexTreeData.p_period_id == 8,
            IndexTreeData.p_terms == "247783,741917",
            IndexTreeData.p_term_id == 247783,
            IndexTreeData.p_dicIds == "67,749",
            IndexTreeData.idx == 0
        )
    )
    
    result = db.execute(query).scalars().all()
    return build_tree(result)