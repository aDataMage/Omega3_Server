from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas.ReturnsSchema import Return
from app.schemas.ReturnsSchema import ReturnCreate, ReturnUpdate
from app.crud import ReturnsCrud as return_crud
from app.db.session import get_db

router = APIRouter()


@router.post("/", response_model=Return)
def create_return(return_: ReturnCreate, db: Session = Depends(get_db)):
    return return_crud.create_return(db=db, return_=return_)


@router.get("/", response_model=list[Return])
def get_returns(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    returns = return_crud.get_returns(db=db, skip=skip, limit=limit)
    return returns


@router.get("/{return_id}", response_model=Return)
def get_return(return_id: str, db: Session = Depends(get_db)):
    db_return = return_crud.get_return_by_id(db=db, return_id=return_id)
    if db_return is None:
        raise HTTPException(status_code=404, detail="Return not found")
    return db_return


@router.put("/{return_id}", response_model=Return)
def update_return(return_id: str, return_: ReturnUpdate, db: Session = Depends(get_db)):
    db_return = return_crud.update_return(db=db, return_id=return_id, return_=return_)
    if db_return is None:
        raise HTTPException(status_code=404, detail="Return not found")
    return db_return


@router.delete("/{return_id}", response_model=Return)
def delete_return(return_id: str, db: Session = Depends(get_db)):
    db_return = return_crud.delete_return(db=db, return_id=return_id)
    if db_return is None:
        raise HTTPException(status_code=404, detail="Return not found")
    return db_return
