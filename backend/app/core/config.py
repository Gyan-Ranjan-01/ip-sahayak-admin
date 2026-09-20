from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "IP-SAKTI Sahayak"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    
    # Secret Keys (Loaded from environment, DO NOT hardcode)
    OPENAI_API_KEY: str = ""
    BHASHINI_API_KEY: str = ""
    
    # Data Sources
    RAW_DATA_DRIVE_URL: str = ""

    # Embeddings (must match the retrieve/egestion service exactly)
    EMBEDDING_MODEL_NAME: str = "BAAI/bge-small-en-v1.5"
    EMBEDDING_DIMENSION: int = 384
    QDRANT_COLLECTION_NAME: str = "legal_chunks"
    
    # Database URLs
    QDRANT_URL: str = "http://localhost:6333"
    QDRANT_API_KEY: str = ""
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASS: str = "ipsakti_secret_password"
    
    # Celery and Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(env_file=(".env", "../.env"), case_sensitive=True, extra="ignore")


settings = Settings()
