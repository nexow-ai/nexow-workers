"""Background worker for agent execution."""

import asyncio
import json
import redis.asyncio as redis
import structlog
from pydantic_settings import BaseSettings

logger = structlog.get_logger(__name__)


class Settings(BaseSettings):
    """Worker settings."""
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    redis_url: str = "redis://localhost:6379"
    redis_channel: str = "nexow:market:prices"
    nexow_agents_url: str = "http://localhost:8002"
    nexow_data_url: str = "http://localhost:8001"
    tick_interval_seconds: int = 5


settings = Settings()


class AgentWorker:
    """
    Background worker that executes trading agents.
    
    Subscribes to Redis for price updates and executes agent logic.
    """

    def __init__(self):
        self.redis_client: redis.Redis | None = None
        self.running = False

    async def start(self):
        """Start the worker."""
        self.redis_client = redis.from_url(settings.redis_url, decode_responses=True)
        self.running = True
        
        logger.info("worker_started")
        
        # Subscribe to price updates
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(settings.redis_channel)
        
        try:
            async for message in pubsub.listen():
                if not self.running:
                    break
                    
                if message["type"] == "message":
                    await self._handle_price_update(message["data"])
                    
        except Exception as e:
            logger.error("worker_error", error=str(e))
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Stop the worker."""
        self.running = False
        if self.redis_client:
            await self.redis_client.aclose()
        logger.info("worker_stopped")

    async def _handle_price_update(self, data: str):
        """Handle incoming price update."""
        try:
            price_data = json.loads(data)
            logger.debug("price_update_received", prices=price_data.get("prices"))
            
            # TODO: Fetch active agents and execute strategies
            # TODO: Generate trading signals
            # TODO: Call nexow-data to execute trades
            
        except Exception as e:
            logger.error("price_update_failed", error=str(e))


async def main():
    """Main entry point."""
    worker = AgentWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
