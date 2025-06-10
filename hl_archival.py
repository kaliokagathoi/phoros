
import asyncio
import aiomysql
import json
import logging
import time
import os
from collections import deque
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BBORecord:
    """BBO data record structure"""
    coin: str
    time: int
    bid_px: Optional[str]
    bid_sz: Optional[str]
    bid_n: Optional[int]
    ask_px: Optional[str]
    ask_sz: Optional[str]
    ask_n: Optional[int]


class BBOArchiver:
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 3306,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 database: str = 'hyperliquid',
                 batch_interval: int = 60,  # seconds
                 max_batch_size: int = 10000):

        # Get credentials from environment variables
        db_user = user or os.getenv('MYSQL_USER')
        db_password = password or os.getenv('MYSQL_PASSWORD')

        if not db_user or not db_password:
            raise ValueError("MySQL credentials not found. Set MYSQL_USER and MYSQL_PASSWORD environment variables.")

        self.db_config = {
            'host': host,
            'port': port,
            'user': db_user,
            'password': db_password,
            'db': database,
            'charset': 'utf8mb4',
            'autocommit': False
        }

        self.batch_interval = batch_interval
        self.max_batch_size = max_batch_size
        self.data_buffer: deque = deque()
        self.pool: Optional[aiomysql.Pool] = None
        self.is_running = False

    async def initialise(self) -> None:
        """initialise database connection pool and create table"""
        try:
            # Create connection pool for high performance
            self.pool = await aiomysql.create_pool(
                minsize=2,
                maxsize=10,
                **self.db_config
            )

            # Create table if it doesn't exist
            await self._create_table()
            logger.info("Database initialised successfully")

        except Exception as e:
            logger.error(f"Failed to initialise database: {e}")
            raise

    async def _create_table(self) -> None:
        """Create BBO table with optimized structure"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bbo (
            coin VARCHAR(20) NOT NULL,
            time BIGINT NOT NULL,
            bid_px DECIMAL(20,8),
            bid_sz DECIMAL(20,8),
            bid_n INT,
            ask_px DECIMAL(20,8), 
            ask_sz DECIMAL(20,8),
            ask_n INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (coin, time),
            INDEX idx_time (time),
            INDEX idx_coin (coin),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB 
          CHARACTER SET=utf8mb4 
          COLLATE=utf8mb4_unicode_ci
          ROW_FORMAT=COMPRESSED;
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(create_table_sql)
                await conn.commit()
                logger.info("BBO table created/verified")

    def process_bbo_message(self, message: Dict[str, Any]) -> None:
        """Process incoming BBO WebSocket message"""
        try:
            if message.get('channel') != 'bbo':
                return

            data = message.get('data', {})
            coin = data.get('coin')
            time_ms = data.get('time')
            bbo = data.get('bbo', [])

            if not coin or not time_ms:
                logger.warning(f"Invalid BBO message: {message}")
                return

            # Extract bid (index 0) and ask (index 1) 
            bid = bbo[0] if len(bbo) > 0 and bbo[0] else None
            ask = bbo[1] if len(bbo) > 1 and bbo[1] else None

            record = BBORecord(
                coin=coin,
                time=time_ms,
                bid_px=bid.get('px') if bid else None,
                bid_sz=bid.get('sz') if bid else None,
                bid_n=bid.get('n') if bid else None,
                ask_px=ask.get('px') if ask else None,
                ask_sz=ask.get('sz') if ask else None,
                ask_n=ask.get('n') if ask else None
            )

            # Add to buffer (thread-safe deque)
            self.data_buffer.append(record)

            # Check if we need to flush early due to buffer size
            if len(self.data_buffer) >= self.max_batch_size:
                asyncio.create_task(self._flush_buffer())

        except Exception as e:
            logger.error(f"Error processing BBO message: {e}")

    async def _flush_buffer(self) -> None:
        """Flush data buffer to database"""
        if not self.data_buffer or not self.pool:
            return

        # Extract all current records (atomic operation)
        records_to_save = []
        while self.data_buffer:
            try:
                records_to_save.append(self.data_buffer.popleft())
            except IndexError:
                break

        if not records_to_save:
            return

        start_time = time.time()

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Use INSERT IGNORE to handle duplicate timestamps gracefully
                    sql = """
                    INSERT IGNORE INTO bbo 
                    (coin, time, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # Prepare batch data
                    batch_data = [
                        (r.coin, r.time, r.bid_px, r.bid_sz, r.bid_n,
                         r.ask_px, r.ask_sz, r.ask_n)
                        for r in records_to_save
                    ]

                    # Execute batch insert
                    await cursor.executemany(sql, batch_data)
                    await conn.commit()

                    elapsed = time.time() - start_time
                    logger.info(f"Saved {len(records_to_save)} BBO records in {elapsed:.3f}s")

        except Exception as e:
            logger.error(f"Failed to save BBO data: {e}")
            # Re-add records to front of buffer for retry
            for record in reversed(records_to_save):
                self.data_buffer.appendleft(record)

    async def start_archiving(self) -> None:
        """Start the periodic archiving process"""
        self.is_running = True
        logger.info(f"Started BBO archiving with {self.batch_interval}s intervals")

        while self.is_running:
            try:
                await asyncio.sleep(self.batch_interval)
                await self._flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in archiving loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retry

    async def stop_archiving(self) -> None:
        """Stop archiving and flush remaining data"""
        self.is_running = False

        # Final flush
        await self._flush_buffer()

        # Close pool
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

        logger.info("BBO archiving stopped")


# Example usage combining with DataFeeds
import websockets


async def websocket_with_reconnect(archiver, max_retries=None):
    """WebSocket connection with automatic reconnection"""
    url = 'wss://api.hyperliquid.xyz/ws'
    retry_count = 0
    coins = ['BTC', 'ETH', 'SOL', 'HYPE', 'TRUMP', 'SOPH', 'BLAST', 'IO', 'AIXBT', 'ZRO', 'SPX', 'MAVIA', 'REQ', 'USTC', 'PENDLE']

    while max_retries is None or retry_count < max_retries:
        try:
            logger.info(f"Connecting to WebSocket (attempt {retry_count + 1})")

            # Connect with explicit timeouts
            async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=2 ** 20,  # 1MB max message size
                    compression=None
            ) as websocket:

                logger.info("WebSocket connected successfully")
                retry_count = 0  # Reset retry count on successful connection

                # Subscribe to BBO feeds
                for coin in coins:
                    subscription = {
                        "method": "subscribe",
                        "subscription": {"type": "bbo", "coin": coin}
                    }
                    await websocket.send(json.dumps(subscription))
                    logger.info(f"Subscribed to BBO for {coin}")
                    await asyncio.sleep(0.1)

                # Process incoming messages
                async for message in websocket:
                    try:
                        if message == "Websocket connection established.":
                            logger.info("WebSocket handshake completed")
                            continue

                        data = json.loads(message)

                        # Archive BBO data
                        if data.get('channel') == 'bbo':
                            archiver.process_bbo_message(data)

                        elif data.get('channel') == 'subscriptionResponse':
                            logger.info(f"Subscription confirmed: {data}")

                        elif data.get('channel') == 'pong':
                            logger.debug("Received pong")

                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON message: {message}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue

        except websockets.exceptions.ConnectionClosed as e:
            retry_count += 1
            logger.warning(f"WebSocket connection closed: {e}. Retrying in 5 seconds... (attempt {retry_count})")
            await asyncio.sleep(5)

        except websockets.exceptions.InvalidHandshake as e:
            retry_count += 1
            logger.error(f"WebSocket handshake failed: {e}. Retrying in 10 seconds... (attempt {retry_count})")
            await asyncio.sleep(10)

        except OSError as e:
            retry_count += 1
            logger.error(f"Network error: {e}. Retrying in 10 seconds... (attempt {retry_count})")
            await asyncio.sleep(10)

        except Exception as e:
            retry_count += 1
            logger.error(f"Unexpected WebSocket error: {e}. Retrying in 15 seconds... (attempt {retry_count})")
            await asyncio.sleep(15)

    logger.error(f"Failed to connect after {max_retries} attempts")


async def archive_bbo_data():
    """
    Main function to archive BBO data from WebSocket stream with robust error handling

    DATA FLOW:
    1. Connect to Hyperliquid WebSocket with auto-reconnection
    2. Subscribe to BBO feeds for multiple coins
    3. WebSocket sends real-time BBO updates
    4. Each message processed by archiver.process_bbo_message()
    5. Data stored in memory buffer (deque)
    6. Every 60 seconds, buffer is flushed to MySQL database
    """

    # initialise archiver (credentials from environment variables)
    archiver = BBOArchiver(
        host='localhost',
        database='hyperliquid',
        batch_interval=60  # Save every 60 seconds
    )

    try:
        # initialise database
        await archiver.initialise()

        # Start archiving task
        archive_task = asyncio.create_task(archiver.start_archiving())

        # Start WebSocket with reconnection
        websocket_task = asyncio.create_task(websocket_with_reconnect(archiver))

        # Wait for either task to complete (or be cancelled)
        try:
            await asyncio.gather(archive_task, websocket_task)
        except asyncio.CancelledError:
            logger.info("Tasks cancelled")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Archive error: {e}")
    finally:
        await archiver.stop_archiving()


if __name__ == "__main__":
    # Check environment variables first
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')

    if not mysql_user or not mysql_password:
        print("ERROR: MySQL credentials not found!")
        print("Please set environment variables:")
        print("  Command Prompt: set MYSQL_USER=your_username")
        print("  Command Prompt: set MYSQL_PASSWORD=your_password")
        print("  PowerShell: $env:MYSQL_USER=\"your_username\"")
        print("  PowerShell: $env:MYSQL_PASSWORD=\"your_password\"")
        exit(1)

    print(f"Found MySQL credentials for user: {mysql_user}")
    print("BBO Data Archiver Starting...")

    asyncio.run(archive_bbo_data())