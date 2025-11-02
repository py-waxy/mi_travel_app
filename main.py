from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import attractions

# Import init_db to verify/connect to the local Postgres on startup
from api.core.database import init_db

app = FastAPI(title="Michigan Travel Planner API")

# Add CORS middleware to allow requests from the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Initialize DB connection (runs once when the app starts)."""
    # This will raise if the DB is unreachable so you get fast feedback
    await init_db()

@app.get("/")
async def root():
    return {"message": "Welcome to the Michigan Travel Planner API"}

app.include_router(attractions.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
