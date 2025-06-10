import asyncio
import websockets
import json
import logging
from typing import Dict, Any, Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFeeds:
    def __init__(self, url: str = 'wss://api.hyperliquid.xyz/ws'):
        self.url = url
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        self.is_connected = False
        self.ping_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Establish WebSocket connection"""
        try:
            self.websocket = await websockets.connect(self.url)
            self.is_connected = True
            logger.info(f"Connected to {self.url}")

            # Start periodic ping
            self.ping_task = asyncio.create_task(self._periodic_ping())
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    async def _subscribe(self, subscription: Dict[str, Any]) -> None:
        """Send subscription message"""
        if not self.is_connected or not self.websocket:
            raise ConnectionError("WebSocket not connected")

        message = {
            "method": "subscribe",
            "subscription": subscription
        }

        try:
            await self.websocket.send(json.dumps(message))
            logger.info(f"Subscribed to: {subscription}")
        except Exception as e:
            logger.error(f"Failed to send subscription: {e}")
            raise

    async def _periodic_ping(self) -> None:
        """Send periodic ping to keep connection alive"""
        while self.is_connected:
            try:
                await asyncio.sleep(30)
                if self.is_connected and self.websocket:
                    await self.websocket.send(json.dumps({"method": "ping"}))
                    logger.debug("Sent ping")
            except Exception as e:
                logger.error(f"Ping failed: {e}")
                break

    async def l2_book(self, coin: str) -> None:
        """Subscribe to L2 order book"""
        await self._subscribe({"type": "l2Book", "coin": coin})

    async def candle(self, coin: str, interval: str) -> None:
        """Subscribe to candle data"""
        await self._subscribe({"type": "candle", "coin": coin, "interval": interval})

    async def all_mids(self, dex: Optional[str] = None) -> None:
        """Subscribe to all mids"""
        subscription = {"type": "allMids"}
        if dex:
            subscription["dex"] = dex
        await self._subscribe(subscription)

    async def trades(self, coin: str) -> None:
        """Subscribe to trades"""
        await self._subscribe({"type": "trades", "coin": coin})

    async def bbo(self, coin: str) -> None:
        """Subscribe to best bid/offer"""
        await self._subscribe({"type": "bbo", "coin": coin})

    async def listen(self) -> None:
        """Listen for incoming messages"""
        if not self.websocket:
            raise ConnectionError("WebSocket not connected")

        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    print(data)
                except json.JSONDecodeError:
                    print(f"Non-JSON message: {message}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error in listen loop: {e}")
            self.is_connected = False

    async def disconnect(self) -> None:
        """Close WebSocket connection"""
        self.is_connected = False

        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass

        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket disconnected")


async def main():
    feeds = DataFeeds()

    try:
        await feeds.connect()
        await asyncio.sleep(1)

        # Subscribe
        await feeds.bbo('UMA')
        await feeds.bbo('BLAST')

        # Listen for messages
        await feeds.listen()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await feeds.disconnect()


if __name__ == "__main__":
    asyncio.run(main())