from fastapi import APIRouter, HTTPException, status, Depends, Cookie
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base, UserData  # Ваши настройки подключения к БД

router = APIRouter()

# Создаём таблицы, если они ещё не созданы
Base.metadata.create_all(bind=engine)

# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.delete("/delete-data/{item_id}", status_code=status.HTTP_200_OK)
def delete_item(
    item_id: int,
    db: Session = Depends(get_db),
    user_id: int = Cookie(None)  # Извлекаем user_id из cookie
):
    """
    Endpoint для удаления элемента. Перед удалением проверяется,
    что user_id, переданный в cookie, совпадает с владельцем элемента.
    """
    print(user_id)
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неавторизованный запрос: отсутствует идентификатор пользователя в cookie."
        )

    # Ищем элемент по его id
    item = db.query(UserData).filter(UserData.id == item_id).first()
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Элемент не найден."
        )

    # Сравниваем user_id из cookie и владельца элемента
    # Предполагается, что в модели Item есть поле user_id, указывающее владельца
    if item.user_id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет прав для удаления данного элемента."
        )

    # Удаляем элемент и сохраняем изменения
    db.delete(item)
    db.commit()

    return {"message": "Элемент успешно удалён."}
