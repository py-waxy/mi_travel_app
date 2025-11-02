from pydantic import BaseModel
from typing import Optional, Dict, Any
from uuid import UUID

class AttractionBase(BaseModel):
    """
    Base schema fields that are common to creating and reading.
    FIXED: Removed wiki_summary and wiki_url to match routes.
    """
    name: str
    type: str
    source: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    image_url: Optional[str] = None

class AttractionCreate(AttractionBase):
    """
    Schema used for creating a new attraction (POST).
    We accept lat/lon and convert it to a 'location' point in the route.
    """
    latitude: float
    longitude: float

class AttractionPublic(AttractionBase):
    """
    Schema for data returned to the client (GET).
    Includes the database ID and parsed lat/lon.
    """
    id: UUID
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    image_url: Optional[str] = None

    class Config:
        orm_mode = True # Pydantic v1
        from_attributes = True # Pydantic v2

class AttractionType(BaseModel):
    """
    Schema for returning a single attraction type.
    """
    type: str

