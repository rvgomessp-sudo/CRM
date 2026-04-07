"""Authentication endpoints: login, current user, user bootstrap."""

import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..auth import hash_password, verify_password, create_access_token, get_current_user
from ..models import User, LoginRequest, LoginResponse, UserCreate, UserResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
async def login(data: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == data.username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(data.senha, user.senha_hash):
        raise HTTPException(401, "Usuário ou senha inválidos")
    if not user.ativo:
        raise HTTPException(403, "Usuário desativado")

    token = create_access_token({"sub": user.username, "papel": user.papel})
    return LoginResponse(access_token=token, user=user)


@router.get("/me", response_model=UserResponse)
async def get_me(current: User = Depends(get_current_user)):
    return current


@router.post("/bootstrap", response_model=UserResponse, status_code=201)
async def bootstrap_first_user(
    data: UserCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create the first admin user. Only works when there are zero users in the system.
    After that, this endpoint returns 403."""
    count_q = await db.execute(select(func.count(User.id)))
    if (count_q.scalar() or 0) > 0:
        raise HTTPException(403, "Bootstrap já foi executado. Use /api/users (admin) para criar novos.")

    user = User(
        username=data.username,
        nome_completo=data.nome_completo,
        email=data.email,
        senha_hash=hash_password(data.senha),
        papel="admin",
        ativo=True,
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)
    return user


@router.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    data: UserCreate,
    current: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new user. Requires admin role."""
    if current.papel != "admin":
        raise HTTPException(403, "Apenas admin pode criar usuários")

    existing = await db.execute(select(User).where(User.username == data.username))
    if existing.scalar_one_or_none():
        raise HTTPException(400, "Username já existe")

    user = User(
        username=data.username,
        nome_completo=data.nome_completo,
        email=data.email,
        senha_hash=hash_password(data.senha),
        papel=data.papel,
        ativo=True,
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)
    return user
