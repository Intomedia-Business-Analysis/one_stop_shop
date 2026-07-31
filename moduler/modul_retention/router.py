from fastapi import APIRouter

router = APIRouter()

@router.get("/retention/ping")
def ping():
    return {"message": "pong"}