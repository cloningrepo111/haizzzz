import base64
from typing import List
from bson.objectid import ObjectId
from fastapi import Depends, HTTPException, status
from pydantic import BaseModel
from config import settings
from src.authentication.jwt_auth import AuthJWT
from src.db import User


class Settings(BaseModel):
    authjwt_algorithm: str = settings.JWT_ALGORITHM
    authjwt_decode_algorithms: List[str] = [settings.JWT_ALGORITHM]
    authjwt_token_location: set = {'cookies', 'headers'}
    authjwt_access_cookie_key: str = 'access_token'
    authjwt_refresh_cookie_key: str = 'refresh_token'
    authjwt_cookie_csrf_protect: bool = False
    authjwt_public_key: str = base64.b64decode(
        settings.JWT_PUBLIC_KEY).decode('utf-8')
    authjwt_private_key: str = base64.b64decode(
        settings.JWT_PRIVATE_KEY).decode('utf-8')


@AuthJWT.load_config
def get_config():
    return Settings()


class NotVerified(Exception):
    pass


class UserNotFound(Exception):
    pass


class NotValidOTP(Exception):
    pass


class NotValidID(Exception):
    pass


async def require_user(Authorize: AuthJWT = Depends()):
    user_id: str | int | None = None
    try:
        Authorize.jwt_required()
        user_id = Authorize.get_jwt_subject()
        print(user_id)
        if not ObjectId.is_valid(user_id):
            raise NotValidID("Invalid user id")

        user = await User.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise UserNotFound("User no longer exists")

        if not user.get("verified"):
            raise NotVerified("You are not verified")

        if not user.get("otp_enabled") or not user.get("otp_verified"):
            raise NotValidOTP("2FA required")

        if not user.get("otp_valid"):
            raise NotValidOTP("2FA failed")

        return user_id
    except Exception as e:
        print(e)
        error = e.__class__.__name__
        if error == "MissingTokenError":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="You are not logged in"
            )
        if error == "UserNotFound":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User no longer exists"
            )
        if error == "NotVerified":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Please verify your account"
            )
        if error == "NotValidOTP":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="2FA method not valid"
            )
        if error == "NotValidID":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid user id"
            )
        # For any other exception, set otp_valid to False before re-raising the exception
        if error != "JWTDecodeError" and user_id is not None:
            await User.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {"otp_valid": False}}
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Error! Token or 2FA code not valid"
        )

