from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from constants import USER, PASSWORD, HOST, PORT, DB

# Конфигурация подключения к PostgreSQL (замените данные подключения на свои)
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

# Создаем движок подключения к базе данных
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Определяем SQLAlchemy модель для хранения данных
class UserData(Base):
    __tablename__ = "user_data"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)  # Значение user_id будет извлечено из cookie
    p_index_id = Column(Integer, nullable=False)
    p_period_id = Column(Integer, nullable=False)
    p_terms = Column(String, nullable=False)
    p_term_id = Column(Integer, nullable=False)
    p_dicIds = Column(String, nullable=False)   
    idx = Column(Integer, nullable=False)
    chart_type = Column(String, nullable=False)
    selected_data = Column(String, nullable=False)
    primary_data = Column(String, nullable=False)

# Создаем таблицы (если они ещё не существуют)
Base.metadata.create_all(bind=engine)

# Pydantic модель для валидации входящих данных (без user_id)
class UserDataCreate(BaseModel):
    p_index_id: int
    p_period_id: int
    p_terms: str
    p_term_id: int
    p_dicIds: str
    idx: int
    chart_type: str
    selected_data: str
    primary_data: str 

router = APIRouter()

@router.post("/save-data")
def save_data(data: UserDataCreate, request: Request):
    # Извлекаем user_id из cookie (ожидается, что cookie называется "user_id")
    cookie_user_id = request.cookies.get("user_id")
    if cookie_user_id is None:
        raise HTTPException(status_code=401, detail="Пользователь не аутентифицирован")
    try:
        user_id = int(cookie_user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Некорректное значение user_id в cookie")

    db = SessionLocal()
    try:
        # Создаем объект модели для сохранения данных в БД, включая user_id из cookie
        db_data = UserData(
            user_id=user_id,
            p_index_id=data.p_index_id,
            p_period_id=data.p_period_id,
            p_terms=data.p_terms,
            p_term_id=data.p_term_id,
            p_dicIds=data.p_dicIds,
            idx=data.idx,
            chart_type=data.chart_type,
            selected_data=data.selected_data,
            primary_data=data.primary_data
        )
        db.add(db_data)
        db.commit()
        db.refresh(db_data)  # Обновляем объект для получения сгенерированного id
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения данных: {e}")
    finally:
        db.close()

    return {"message": "Данные успешно сохранены", "data_id": db_data.id}