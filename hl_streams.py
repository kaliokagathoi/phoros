import asyncio
import websockets
import zmq
import zmq.asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List, Set
from datetime import datetime
import sys

# Fix for Windows ZeroMQ + asyncio compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFeeds:
    def __init__(self,
                 ws_url: str = 'wss://api.hyperliquid.xyz/ws',
                 zmq_port: int = 5555,
                 zmq_address: str = "tcp://*"):

        self.ws_url = ws_url
        self.zmq_address = f"{zmq_address}:{zmq_port}"

        # WebSocket connection
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        self.is_connected = False

        # ZeroMQ publisher
        self.zmq_context = zmq.asyncio.Context()
        self.zmq_publisher = self.zmq_context.socket(zmq.PUB)

        # Subscription management
        self.subscriptions: Set[str] = set()
        self.subscription_queue: List[Dict[str, Any]] = []

        # Tasks
        self.websocket_task: Optional[asyncio.Task] = None
        self.ping_task: Optional[asyncio.Task] = None

        # Statistics
        self.message_count = 0
        self.last_message_time = 0
        self.start_time = time.time()

    async def start(self) -> None:
        """Start the DataFeeds service"""
        try:
            # Bind ZeroMQ publisher
            self.zmq_publisher.bind(self.zmq_address)
            logger.info(f"ZeroMQ publisher bound to {self.zmq_address}")

            # Start WebSocket connection with reconnection
            self.websocket_task = asyncio.create_task(self._websocket_handler())

            # Start ping task
            self.ping_task = asyncio.create_task(self._ping_handler())

            logger.info("DataFeeds service started")

            # Keep running
            await asyncio.gather(self.websocket_task, self.ping_task)

        except Exception as e:
            logger.error(f"Failed to start DataFeeds: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the DataFeeds service"""
        logger.info("Stopping DataFeeds service...")

        # Cancel tasks
        if self.websocket_task:
            self.websocket_task.cancel()
        if self.ping_task:
            self.ping_task.cancel()

        # Close WebSocket
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()

        # Close ZeroMQ
        self.zmq_publisher.close()
        self.zmq_context.term()

        logger.info("DataFeeds service stopped")

    # Subscription methods for different data types
    async def subscribe_bbo(self, coin: str) -> None:
        """Subscribe to best bid/offer for a coin"""
        await self._add_subscription({"type": "bbo", "coin": coin})

    async def subscribe_l2_book(self, coin: str) -> None:
        """Subscribe to L2 order book for a coin"""
        await self._add_subscription({"type": "l2Book", "coin": coin})

    async def subscribe_trades(self, coin: str) -> None:
        """Subscribe to trades for a coin"""
        await self._add_subscription({"type": "trades", "coin": coin})

    async def subscribe_candle(self, coin: str, interval: str) -> None:
        """Subscribe to candle data for a coin (REMOVED - not needed)"""
        pass  # Removed candle subscriptions

    async def subscribe_all_mids(self, dex: Optional[str] = None) -> None:
        """Subscribe to all mid prices"""
        subscription = {"type": "allMids"}
        if dex:
            subscription["dex"] = dex
        await self._add_subscription(subscription)

    async def _add_subscription(self, subscription: Dict[str, Any]) -> None:
        """Add a subscription and send immediately if connected"""
        sub_key = self._subscription_key(subscription)
        if sub_key not in self.subscriptions:
            self.subscriptions.add(sub_key)

            # If connected, send immediately; otherwise queue it
            if self.is_connected and self.websocket:
                try:
                    message = {
                        "method": "subscribe",
                        "subscription": subscription
                    }
                    await self.websocket.send(json.dumps(message))
                    logger.info(f"Sent subscription: {subscription}")
                    await asyncio.sleep(0.1)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to send subscription {subscription}: {e}")
                    # Add to queue for retry
                    self.subscription_queue.append(subscription)
            else:
                self.subscription_queue.append(subscription)
                logger.info(f"Queued subscription: {subscription}")

    def _subscription_key(self, subscription: Dict[str, Any]) -> str:
        """Generate unique key for subscription"""
        return json.dumps(subscription, sort_keys=True)

    async def _websocket_handler(self) -> None:
        """Handle WebSocket connection with auto-reconnection"""
        retry_count = 0
        max_retries = None  # Infinite retries

        while max_retries is None or retry_count < max_retries:
            try:
                logger.info(f"Connecting to WebSocket (attempt {retry_count + 1})")

                async with websockets.connect(
                        self.ws_url,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=10,
                        max_size=2 ** 20,
                        compression=None
                ) as websocket:

                    self.websocket = websocket
                    self.is_connected = True
                    logger.info("WebSocket connected successfully")
                    retry_count = 0

                    # Process queued subscriptions
                    await self._process_subscription_queue()

                    # Listen for messages
                    async for message in websocket:
                        try:
                            await self._process_message(message)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue

            except websockets.exceptions.ConnectionClosed as e:
                self.is_connected = False
                retry_count += 1
                logger.warning(f"WebSocket connection closed: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

            except Exception as e:
                self.is_connected = False
                retry_count += 1
                logger.error(f"WebSocket error: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)

        logger.error("WebSocket handler exited")

    async def _process_subscription_queue(self) -> None:
        """Send all queued subscriptions"""
        if not self.subscription_queue or not self.websocket:
            logger.info("No subscriptions to process or WebSocket not available")
            return

        logger.info(f"Processing {len(self.subscription_queue)} queued subscriptions")

        for subscription in self.subscription_queue:
            try:
                message = {
                    "method": "subscribe",
                    "subscription": subscription
                }
                await self.websocket.send(json.dumps(message))
                logger.info(f"Sent subscription: {subscription}")
                await asyncio.sleep(0.1)  # Rate limiting
            except Exception as e:
                logger.error(f"Failed to send subscription {subscription}: {e}")

        self.subscription_queue.clear()
        logger.info("Finished processing subscription queue")

    async def _process_message(self, message: str) -> None:
        """Process incoming WebSocket message and publish to ZeroMQ"""
        try:
            # Handle handshake message
            if message == "Websocket connection established.":
                logger.info("WebSocket handshake completed")
                return

            # Parse JSON
            data = json.loads(message)

            # Update statistics
            self.message_count += 1
            self.last_message_time = time.time()

            # Handle dict messages (all Hyperliquid messages are dicts)
            if isinstance(data, dict):
                channel = data.get('channel')

                # Special handling for trades channel with array data
                if channel == 'trades':
                    trades_data = data.get('data', [])
                    if isinstance(trades_data, list):
                        # Send each trade individually
                        for trade in trades_data:
                            if isinstance(trade, dict):
                                topic = f"trades.{trade.get('coin', 'unknown')}"
                                zmq_message = {
                                    "topic": topic,
                                    "timestamp": time.time(),
                                    "data": trade  # Individual trade data
                                }
                                await self._publish_zmq(topic, zmq_message)
                    return

                # Handle other message types (BBO, L2Book, subscriptions, etc.)
                topic = self._get_topic(data)
                if topic:
                    # Prepare message for ZeroMQ
                    zmq_message = {
                        "topic": topic,
                        "timestamp": time.time(),
                        "data": data
                    }

                    # Publish to ZeroMQ
                    await self._publish_zmq(topic, zmq_message)

                    # Log subscription confirmations
                    if data.get('channel') == 'subscriptionResponse':
                        logger.info(f"Subscription confirmed: {data}")
                else:
                    logger.debug(f"No topic found for message: {channel}")

            elif isinstance(data, list):
                # This shouldn't happen with current Hyperliquid API, but handle just in case
                logger.warning(f"Received unexpected list message: {data}")
                for item in data:
                    if isinstance(item, dict):
                        topic = self._get_topic_from_data(item)
                        if topic:
                            zmq_message = {
                                "topic": topic,
                                "timestamp": time.time(),
                                "data": item
                            }
                            await self._publish_zmq(topic, zmq_message)

            else:
                logger.warning(f"Unknown message type: {type(data)} - {data}")

        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message type: {type(data)}")
            logger.error(f"Message content: {str(data)[:500]}")  # First 500 chars for debugging

    def _get_topic_from_data(self, item: Dict[str, Any]) -> Optional[str]:
        """Get topic from individual data item (for list messages)"""
        # For trade items
        if 'coin' in item and 'side' in item and 'px' in item:
            return f"trades.{item['coin']}"

        # For other data types, try to infer
        coin = item.get('coin')
        if coin:
            return f"data.{coin}"

        return None

    def _get_topic(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract topic from message for ZeroMQ routing"""
        channel = data.get('channel')

        if not channel:
            return None

        # Topic format: channel.coin (e.g., "bbo.BTC", "trades.ETH")
        if channel in ['bbo', 'l2Book', 'trades']:
            coin = data.get('data', {}).get('coin')
            if coin:
                return f"{channel}.{coin}"

        elif channel == 'candle':
            candle_data = data.get('data', {})
            coin = candle_data.get('s', '').split('-')[0] if candle_data.get('s') else None
            interval = candle_data.get('i')
            if coin and interval:
                return f"candle.{coin}.{interval}"

        elif channel == 'allMids':
            return "allMids"

        elif channel in ['subscriptionResponse', 'error', 'pong']:
            return f"system.{channel}"

        return channel

    async def _publish_zmq(self, topic: str, message: Dict[str, Any]) -> None:
        """Publish message to ZeroMQ"""
        try:
            # ZeroMQ multipart message: [topic, json_data]
            await self.zmq_publisher.send_multipart([
                topic.encode('utf-8'),
                json.dumps(message).encode('utf-8')
            ])

        except Exception as e:
            logger.error(f"Failed to publish to ZeroMQ: {e}")

    async def _ping_handler(self) -> None:
        """Send periodic pings and print statistics"""
        while True:
            try:
                await asyncio.sleep(30)

                # Send ping if connected
                if self.is_connected and self.websocket:
                    ping_message = {"method": "ping"}
                    await self.websocket.send(json.dumps(ping_message))

                # Print statistics
                uptime = time.time() - self.start_time
                msg_rate = self.message_count / uptime if uptime > 0 else 0

                logger.info(f"Stats: {self.message_count} messages, "
                            f"{msg_rate:.1f} msg/s, "
                            f"uptime: {uptime:.1f}s, "
                            f"connected: {self.is_connected}")

            except Exception as e:
                logger.error(f"Ping handler error: {e}")


# Convenience function for running DataFeeds
async def run_data_feeds():
    """Run DataFeeds with common subscriptions"""
    feeds = DataFeeds()

    try:
        # Add subscriptions BEFORE starting (they'll be queued)
        coins = ['BTC', 'ETH', 'SOL', 'HYPE', 'SPX', 'CAKE', 'REQ', 'TON', 'MAVIA', 'SOPH', 'ACE', 'BLAST', 'ZRO',
                 'WCT']

        logger.info("Adding subscriptions...")
        for coin in coins:
            await feeds.subscribe_bbo(coin)
            await feeds.subscribe_trades(coin)
            await feeds.subscribe_l2_book(coin)
            # Removed candle subscriptions

        #await feeds.subscribe_all_mids()
        logger.info(f"Added {len(feeds.subscription_queue)} subscriptions to queue")

        # Now start the service (will process queued subscriptions when connected)
        await feeds.start()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"DataFeeds error: {e}")
    finally:
        await feeds.stop()


if __name__ == "__main__":
    print("Hyperliquid DataFeeds Publisher")
    print("Publishing market data to ZeroMQ on tcp://*:5555")
    print("Ctrl+C to stop")

    asyncio.run(run_data_feeds())