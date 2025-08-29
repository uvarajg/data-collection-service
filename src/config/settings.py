from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    
    debug: bool = False
    port: int = 8000
    
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    
    gemini_api_key: str = ""
    google_api_key: str = ""
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }
    
    log_level: str = "INFO"


@lru_cache()
def get_settings() -> Settings:
    return Settings()