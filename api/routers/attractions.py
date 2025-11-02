from fastapi import APIRouter, HTTPException, Query, Depends, status
from typing import List
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.attractions import AttractionCreate, AttractionPublic, AttractionType
from api.core.database import get_db
from api.core.models import Attraction

router = APIRouter(prefix="/attractions", tags=["Attractions"])


@router.post("/", response_model=AttractionPublic, status_code=201)
async def create_attraction(attraction: AttractionCreate, db: AsyncSession = Depends(get_db)):
    """Create a new attraction using the local Postgres database."""
    try:
        db_obj = Attraction(
            name=attraction.name,
            type=attraction.type,
            source=attraction.source,
            tags=attraction.tags,
            image_url=attraction.image_url,
            latitude=attraction.latitude,
            longitude=attraction.longitude,
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[AttractionPublic])
async def list_attractions(limit: int = 100, offset: int = 0, db: AsyncSession = Depends(get_db)):
    """List attractions from local Postgres."""
    try:
        stmt = select(Attraction).limit(limit).offset(offset)
        result = await db.execute(stmt)
        items = result.scalars().all()
        return items
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nearby/", response_model=List[AttractionPublic])
async def find_nearby_attractions(
    lat: float = Query(..., example=44.7728),
    lon: float = Query(..., example=-85.5802),
    distance_meters: int = Query(5000, example=5000),
    type: str = Query(None, description="Filter by attraction type"),
    db: AsyncSession = Depends(get_db),
):
    """Find attractions near a lat/lon. This implementation uses a simple bounding box filter.
    For production use, add PostGIS and an indexed geography column for accurate and fast spatial queries.
    """
    try:
        # Approximate degrees for given distance (very rough): ~111000 meters per degree latitude
        deg_distance = distance_meters / 111000.0
        min_lat = lat - deg_distance
        max_lat = lat + deg_distance
        min_lon = lon - deg_distance
        max_lon = lon + deg_distance

        stmt = select(Attraction).where(
            Attraction.latitude >= min_lat,
            Attraction.latitude <= max_lat,
            Attraction.longitude >= min_lon,
            Attraction.longitude <= max_lon,
        )
        if type:
            stmt = stmt.where(Attraction.type == type)
        result = await db.execute(stmt)
        items = result.scalars().all()
        return items
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{attraction_id}", response_model=AttractionPublic)
async def get_attraction(attraction_id: str, db: AsyncSession = Depends(get_db)):
    """Get a single attraction by its UUID."""
    try:
        stmt = select(Attraction).where(Attraction.id == attraction_id)
        result = await db.execute(stmt)
        item = result.scalars().first()
        if not item:
            raise HTTPException(status_code=404, detail="Attraction not found")
        return item
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{attraction_id}", status_code=204)
async def delete_attraction(attraction_id: str, db: AsyncSession = Depends(get_db)):
    """Delete an attraction by UUID."""
    try:
        stmt = select(Attraction).where(Attraction.id == attraction_id)
        result = await db.execute(stmt)
        item = result.scalars().first()
        if not item:
            raise HTTPException(status_code=404, detail="Attraction not found")
        await db.delete(item)
        await db.commit()
        return None
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/types/", response_model=List[AttractionType])
async def list_attraction_types(db: AsyncSession = Depends(get_db)):
    """List unique attraction types."""
    try:
        stmt = select(func.distinct(Attraction.type))
        result = await db.execute(stmt)
        types = [row[0] for row in result.all() if row[0]]
        return [{"type": t} for t in sorted(types)]
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by_type/{type_name}", response_model=List[AttractionPublic])
async def get_attractions_by_type(type_name: str, limit: int = 100, offset: int = 0, db: AsyncSession = Depends(get_db)):
    """Get attractions filtered by type."""
    try:
        stmt = select(Attraction).where(Attraction.type == type_name).limit(limit).offset(offset)
        result = await db.execute(stmt)
        items = result.scalars().all()
        return items
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
