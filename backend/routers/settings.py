from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from backend.database import get_db
from backend.models import Setting

router = APIRouter(prefix="/api/settings", tags=["settings"])

class SettingUpdate(BaseModel):
    value: str
    updated_by: Optional[str] = None

@router.get("")
def get_all_settings(db: Session = Depends(get_db)):
    settings = db.query(Setting).all()
    return {s.key: s.value for s in settings}

@router.put("/{key}")
def update_setting(key: str, data: SettingUpdate, db: Session = Depends(get_db)):
    s = db.query(Setting).filter(Setting.key == key).first()
    if s:
        s.value = data.value
        s.updated_by = data.updated_by
    else:
        s = Setting(key=key, value=data.value, updated_by=data.updated_by)
        db.add(s)
    db.commit()
    return {"key": key, "value": data.value}
