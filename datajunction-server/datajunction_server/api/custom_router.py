"""
Basic OAuth Authentication Router
"""
from datetime import timedelta
import asyncio
from http import HTTPStatus
import os
from alembic.config import Config
from alembic import command

from fastapi import APIRouter, Depends, Form, HTTPException, Header
from fastapi.responses import JSONResponse, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select,text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_user_password,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.utils import Settings, get_session, get_settings
from fastapi import Request

router = APIRouter(tags=["custom router"]) ## for swagger ui


async def run_alembic_upgrade():
    # Set the path to the Alembic configuration file
    alembic_ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'alembic.ini'))

    # Set the path to the Alembic scripts directory
    script_location = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'alembic'))

    # Check if paths exist
    if not os.path.isfile(alembic_ini_path):
        raise FileNotFoundError(f"Config file not found: {alembic_ini_path}")
    if not os.path.isdir(script_location):
        raise FileNotFoundError(f"Script directory not found: {script_location}")

    # Load the Alembic configuration
    alembic_cfg = Config(alembic_ini_path)
    alembic_cfg.set_main_option('script_location', script_location)

    # Run the Alembic upgrade command
    try:
        # This might need to be run in a thread if it’s blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: command.upgrade(alembic_cfg, 'head'))
    except Exception as e:
        print(f"Error during upgrade: {e}")
        raise

@router.post("/schema/register/")
async def schema_register(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Create a new schema if it does not exist.
    """
    try:
        body  = await request.json()
        schema = body.get("schema")
        print("schema body is",schema)
        if not schema:
            raise HTTPException(status_code=400, detail="Schema in body is required")

        # Check if schema already exists
        query = text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name")
        result = await session.execute(query, {"schema_name": schema})
        if result.fetchone():
            raise HTTPException(status_code=400, detail="Schema already exists")

        # Create schema
        await session.execute(text(f"CREATE SCHEMA {schema}"))
        await session.commit()


        await run_alembic_upgrade()

        return JSONResponse(
            content={"message": "Schema successfully created and migrations applied"},
            status_code=HTTPStatus.CREATED,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
