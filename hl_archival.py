from dotenv import load_dotenv
load_dotenv()

import asyncio
import aiomysql
import zmq
import zmq.asyncio
import json
import logging
import time
import os
import warnings
from collections import deque
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
import sys

# Suppress MySQL duplicate entry warnings
warnings.filterwarnings("ignore", message=".*Duplicate entry.*for key.*")
warnings.filterwarnings("ignore", category=aiomysql.Warning)

# Fix for Windows ZeroMQ + asyncio compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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

@dataclass
class TradeRecord:
    """Trade data record structure"""
    coin: str
    side: str
    px: str
    sz: str
    hash: str
    time: int
    tid: int
    buyer: Optional[str]
    seller: Optional[str]

@dataclass
class BookRecord:
    """L2 Book level record structure"""
    coin: str
    time: int
    side: str  # 'bid' or 'ask'
    level: int  # 0=best, 1=second best, etc.
    px: str
    sz: str
    n: int

class ZMQConsumer(ABC):
    """Base class for ZeroMQ consumers"""

    def __init__(self,
                 zmq_address: str = "tcp://localhost:5555",
                 topics: List[str] = None):

        self.zmq_address = zmq_address
        self.topics = topics or []

        # ZeroMQ subscriber
        self.zmq_context = zmq.asyncio.Context()
        self.zmq_subscriber = self.zmq_context.socket(zmq.SUB)

        # Statistics
        self.message_count = 0
        self.start_time = time.time()
        self.is_running = False

    async def start(self) -> None:
        """Start the consumer"""
        try:
            # Connect to ZeroMQ publisher
            self.zmq_subscriber.connect(self.zmq_address)
            logger.info(f"Connected to ZeroMQ at {self.zmq_address}")

            # Subscribe to topics
            if not self.topics:
                # Subscribe to all topics
                self.zmq_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
                logger.info("Subscribed to ALL topics")
            else:
                for topic in self.topics:
                    self.zmq_subscriber.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
                    logger.info(f"Subscribed to topic: {topic}")

            # Initialize consumer-specific resources
            await self.initialize()

            self.is_running = True
            logger.info(f"{self.__class__.__name__} started")

            # Start message processing
            await self._message_loop()

        except Exception as e:
            logger.error(f"Failed to start {self.__class__.__name__}: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the consumer"""
        logger.info(f"Stopping {self.__class__.__name__}...")
        self.is_running = False

        # Cleanup consumer-specific resources
        await self.cleanup()

        # Close ZeroMQ
        self.zmq_subscriber.close()
        self.zmq_context.term()

        logger.info(f"{self.__class__.__name__} stopped")

    async def _message_loop(self) -> None:
        """Main message processing loop"""
        while self.is_running:
            try:
                # Receive multipart message: [topic, json_data]
                parts = await self.zmq_subscriber.recv_multipart(zmq.NOBLOCK)

                if len(parts) != 2:
                    # Skip invalid message format silently
                    continue

                topic = parts[0].decode('utf-8')
                message_data = json.loads(parts[1].decode('utf-8'))

                # Update statistics
                self.message_count += 1

                # Process message
                await self.process_message(topic, message_data)

            except zmq.Again:
                # No message available, sleep briefly
                await asyncio.sleep(0.001)
            except Exception as e:
                logger.error(f"Error in message loop: {e}")
                await asyncio.sleep(0.1)

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize consumer-specific resources"""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup consumer-specific resources"""
        pass

    @abstractmethod
    async def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process received message"""
        pass

class BBOArchiver(ZMQConsumer):
    """Archive BBO data to MySQL"""

    def __init__(self,
                 zmq_address: str = "tcp://localhost:5555",
                 host: str = 'localhost',
                 port: int = 3306,
                 database: str = 'hyperliquid',
                 batch_interval: int = 60,
                 max_batch_size: int = 10000):

        # Subscribe to BBO topics only
        super().__init__(zmq_address, topics=["bbo"])

        # Database configuration
        db_user = os.getenv('MYSQL_USER')
        db_password = os.getenv('MYSQL_PASSWORD')

        if not db_user or not db_password:
            raise ValueError("MYSQL_USER and MYSQL_PASSWORD environment variables required")

        self.db_config = {
            'host': host,
            'port': port,
            'user': db_user,
            'password': db_password,
            'db': database,
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # Archival configuration
        self.batch_interval = batch_interval
        self.max_batch_size = max_batch_size
        self.data_buffer: deque = deque()
        self.pool: Optional[aiomysql.Pool] = None

        # Archival task
        self.archive_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize database connection and start archival task"""
        try:
            # Create connection pool
            self.pool = await aiomysql.create_pool(
                minsize=2,
                maxsize=10,
                **self.db_config
            )

            # Create table
            await self._create_table()

            # Start archival task
            self.archive_task = asyncio.create_task(self._archival_loop())

            logger.info("BBOArchiver initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize BBOArchiver: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup database resources"""
        # Cancel archival task
        if self.archive_task:
            self.archive_task.cancel()
            try:
                await self.archive_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush_buffer()

        # Close database pool
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

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

    async def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process BBO message from ZeroMQ"""
        try:
            # Extract original WebSocket data
            ws_data = message.get('data', {})

            if ws_data.get('channel') != 'bbo':
                return

            bbo_data = ws_data.get('data', {})
            coin = bbo_data.get('coin')
            time_ms = bbo_data.get('time')
            bbo = bbo_data.get('bbo', [])

            if not coin or not time_ms:
                # Skip invalid data silently
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

            # Add to buffer
            self.data_buffer.append(record)

            # Check if buffer needs flushing
            if len(self.data_buffer) >= self.max_batch_size:
                asyncio.create_task(self._flush_buffer())

        except Exception as e:
            logger.error(f"Error processing BBO message: {e}")

    async def _archival_loop(self) -> None:
        """Periodic archival task"""
        while self.is_running:
            try:
                await asyncio.sleep(self.batch_interval)
                await self._flush_buffer()

                # Print statistics
                uptime = time.time() - self.start_time
                msg_rate = self.message_count / uptime if uptime > 0 else 0
                buffer_size = len(self.data_buffer)

                logger.info(f"BBO Stats: {self.message_count} messages, "
                          f"{msg_rate:.1f} msg/s, buffer: {buffer_size}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in archival loop: {e}")
                await asyncio.sleep(5)

    async def _flush_buffer(self) -> None:
        """Flush data buffer to database"""
        if not self.data_buffer or not self.pool:
            return

        # Extract all records
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
                    sql = """
                    INSERT IGNORE INTO bbo 
                    (coin, time, bid_px, bid_sz, bid_n, ask_px, ask_sz, ask_n)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    batch_data = [
                        (r.coin, r.time, r.bid_px, r.bid_sz, r.bid_n,
                         r.ask_px, r.ask_sz, r.ask_n)
                        for r in records_to_save
                    ]

                    await cursor.executemany(sql, batch_data)
                    await conn.commit()

                    elapsed = time.time() - start_time
                    logger.info(f"Saved {len(records_to_save)} BBO records in {elapsed:.3f}s")

        except Exception as e:
            logger.error(f"Failed to save BBO data: {e}")
            # Re-add records for retry
            for record in reversed(records_to_save):
                self.data_buffer.appendleft(record)

class BookArchiver(ZMQConsumer):
    """Archive L2 Book data to MySQL"""

    def __init__(self,
                 zmq_address: str = "tcp://localhost:5555",
                 host: str = 'localhost',
                 port: int = 3306,
                 database: str = 'hyperliquid',
                 batch_interval: int = 60,
                 max_batch_size: int = 50000,  # Higher limit for book data
                 max_levels: int = 10):   # Number of limits to store on each side

        # Subscribe to l2Book topics only
        super().__init__(zmq_address, topics=["l2Book"])

        # Database configuration
        db_user = os.getenv('MYSQL_USER')
        db_password = os.getenv('MYSQL_PASSWORD')

        if not db_user or not db_password:
            raise ValueError("MYSQL_USER and MYSQL_PASSWORD environment variables required")

        self.db_config = {
            'host': host,
            'port': port,
            'user': db_user,
            'password': db_password,
            'db': database,
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # Archival configuration
        self.batch_interval = batch_interval
        self.max_batch_size = max_batch_size
        self.max_levels = max_levels
        self.data_buffer: deque = deque()
        self.pool: Optional[aiomysql.Pool] = None

        # Archival task
        self.archive_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize database connection and start archival task"""
        try:
            # Create connection pool
            self.pool = await aiomysql.create_pool(
                minsize=2,
                maxsize=10,
                **self.db_config
            )

            # Create table
            await self._create_table()

            # Start archival task
            self.archive_task = asyncio.create_task(self._archival_loop())

            logger.info("BookArchiver initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize BookArchiver: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup database resources"""
        # Cancel archival task
        if self.archive_task:
            self.archive_task.cancel()
            try:
                await self.archive_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush_buffer()

        # Close database pool
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def _create_table(self) -> None:
        """Create book levels table with optimized structure"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS book (
            coin VARCHAR(20) NOT NULL,
            time BIGINT NOT NULL,
            side ENUM('bid', 'ask') NOT NULL,
            level TINYINT UNSIGNED NOT NULL,
            px DECIMAL(20,8) NOT NULL,
            sz DECIMAL(20,8) NOT NULL,
            n INT UNSIGNED NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (coin, time, side, level),
            INDEX idx_time (time),
            INDEX idx_coin (coin),
            INDEX idx_side (side),
            INDEX idx_level (level),
            INDEX idx_coin_time (coin, time),
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
                logger.info("Book levels table created/verified")

    async def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process L2 book message from ZeroMQ"""
        try:
            # Extract original WebSocket data
            ws_data = message.get('data', {})

            if ws_data.get('channel') != 'l2Book':
                return

            book_data = ws_data.get('data', {})
            coin = book_data.get('coin')
            time_ms = book_data.get('time')
            levels = book_data.get('levels', [])

            if not coin or not time_ms or len(levels) < 2:
                # Skip invalid data silently
                return

            # Process bids (levels[0]) and asks (levels[1])
            bids = levels[0] if len(levels) > 0 else []
            asks = levels[1] if len(levels) > 1 else []

            # Process bid levels
            for level_idx, bid in enumerate(bids[:self.max_levels]):
                if isinstance(bid, dict) and all(k in bid for k in ['px', 'sz', 'n']):
                    record = BookRecord(
                        coin=coin,
                        time=time_ms,
                        side='bid',
                        level=level_idx,
                        px=str(bid['px']),
                        sz=str(bid['sz']),
                        n=int(bid['n'])
                    )
                    self.data_buffer.append(record)

            # Process ask levels
            for level_idx, ask in enumerate(asks[:self.max_levels]):
                if isinstance(ask, dict) and all(k in ask for k in ['px', 'sz', 'n']):
                    record = BookRecord(
                        coin=coin,
                        time=time_ms,
                        side='ask',
                        level=level_idx,
                        px=str(ask['px']),
                        sz=str(ask['sz']),
                        n=int(ask['n'])
                    )
                    self.data_buffer.append(record)

            # Check if buffer needs flushing
            if len(self.data_buffer) >= self.max_batch_size:
                asyncio.create_task(self._flush_buffer())

        except Exception as e:
            logger.error(f"Error processing book message: {e}")

    async def _archival_loop(self) -> None:
        """Periodic archival task"""
        while self.is_running:
            try:
                await asyncio.sleep(self.batch_interval)
                await self._flush_buffer()

                # Print statistics
                uptime = time.time() - self.start_time
                msg_rate = self.message_count / uptime if uptime > 0 else 0
                buffer_size = len(self.data_buffer)

                logger.info(f"Book Stats: {self.message_count} messages, "
                          f"{msg_rate:.1f} msg/s, buffer: {buffer_size}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in book archival loop: {e}")
                await asyncio.sleep(5)

    async def _flush_buffer(self) -> None:
        """Flush data buffer to database"""
        if not self.data_buffer or not self.pool:
            return

        # Extract all records
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
                    sql = """
                    INSERT IGNORE INTO book 
                    (coin, time, side, level, px, sz, n)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

                    batch_data = [
                        (r.coin, r.time, r.side, r.level, r.px, r.sz, r.n)
                        for r in records_to_save
                    ]

                    await cursor.executemany(sql, batch_data)
                    await conn.commit()

                    elapsed = time.time() - start_time
                    logger.info(f"Saved {len(records_to_save)} book level records in {elapsed:.3f}s")

        except Exception as e:
            logger.error(f"Failed to save book data: {e}")
            # Re-add records for retry
            for record in reversed(records_to_save):
                self.data_buffer.appendleft(record)

class TradesArchiver(ZMQConsumer):
    """Archive Trades data to MySQL"""

    def __init__(self,
                 zmq_address: str = "tcp://localhost:5555",
                 host: str = 'localhost',
                 port: int = 3306,
                 database: str = 'hyperliquid',
                 batch_interval: int = 60,
                 max_batch_size: int = 10000):

        # Subscribe to trades topics only
        super().__init__(zmq_address, topics=["trades"])

        # Database configuration
        db_user = os.getenv('MYSQL_USER')
        db_password = os.getenv('MYSQL_PASSWORD')

        if not db_user or not db_password:
            raise ValueError("MYSQL_USER and MYSQL_PASSWORD environment variables required")

        self.db_config = {
            'host': host,
            'port': port,
            'user': db_user,
            'password': db_password,
            'db': database,
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # Archival configuration
        self.batch_interval = batch_interval
        self.max_batch_size = max_batch_size
        self.data_buffer: deque = deque()
        self.pool: Optional[aiomysql.Pool] = None

        # Archival task
        self.archive_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize database connection and start archival task"""
        try:
            # Create connection pool
            self.pool = await aiomysql.create_pool(
                minsize=2,
                maxsize=10,
                **self.db_config
            )

            # Create table
            await self._create_table()

            # Start archival task
            self.archive_task = asyncio.create_task(self._archival_loop())

            logger.info("TradesArchiver initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize TradesArchiver: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup database resources"""
        # Cancel archival task
        if self.archive_task:
            self.archive_task.cancel()
            try:
                await self.archive_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush_buffer()

        # Close database pool
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def _create_table(self) -> None:
        """Create trades table with optimized structure"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS trades (
            coin VARCHAR(20) NOT NULL,
            side VARCHAR(4) NOT NULL,
            px DECIMAL(20,8) NOT NULL,
            sz DECIMAL(20,8) NOT NULL,
            hash VARCHAR(128) NOT NULL,
            time BIGINT NOT NULL,
            tid BIGINT NOT NULL,
            buyer VARCHAR(66),
            seller VARCHAR(66),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (coin, time, tid),
            INDEX idx_time (time),
            INDEX idx_coin (coin),
            INDEX idx_side (side),
            INDEX idx_hash (hash),
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
                logger.info("Trades table created/verified")

    async def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process trade message from ZeroMQ"""
        try:
            # Handle direct trade data (from individual trade processing in hl_streams.py)
            trade_data = message.get('data', {})

            if isinstance(trade_data, dict) and 'coin' in trade_data and 'side' in trade_data:
                self._process_single_trade(trade_data)

        except Exception as e:
            logger.error(f"Error processing trade message: {e}")

    def _process_single_trade(self, trade_data: Dict[str, Any]) -> None:
        """Process a single trade record"""
        try:
            users = trade_data.get('users', [None, None])
            buyer = users[0] if len(users) > 0 else None
            seller = users[1] if len(users) > 1 else None

            record = TradeRecord(
                coin=trade_data.get('coin', ''),
                side=trade_data.get('side', ''),
                px=trade_data.get('px', '0'),
                sz=trade_data.get('sz', '0'),
                hash=trade_data.get('hash', ''),
                time=trade_data.get('time', 0),
                tid=trade_data.get('tid', 0),
                buyer=buyer,
                seller=seller
            )

            # Add to buffer
            self.data_buffer.append(record)

            # Check if buffer needs flushing
            if len(self.data_buffer) >= self.max_batch_size:
                asyncio.create_task(self._flush_buffer())

        except Exception as e:
            logger.error(f"Error processing single trade: {e}")

    async def _archival_loop(self) -> None:
        """Periodic archival task"""
        while self.is_running:
            try:
                await asyncio.sleep(self.batch_interval)
                await self._flush_buffer()

                # Print statistics
                uptime = time.time() - self.start_time
                msg_rate = self.message_count / uptime if uptime > 0 else 0
                buffer_size = len(self.data_buffer)

                logger.info(f"Trades Stats: {self.message_count} messages, "
                          f"{msg_rate:.1f} msg/s, buffer: {buffer_size}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in trades archival loop: {e}")
                await asyncio.sleep(5)

    async def _flush_buffer(self) -> None:
        """Flush data buffer to database"""
        if not self.data_buffer or not self.pool:
            return

        # Extract all records
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
                    sql = """
                    INSERT IGNORE INTO trades 
                    (coin, side, px, sz, hash, time, tid, buyer, seller)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    batch_data = [
                        (r.coin, r.side, r.px, r.sz, r.hash, r.time, r.tid, r.buyer, r.seller)
                        for r in records_to_save
                    ]

                    await cursor.executemany(sql, batch_data)
                    await conn.commit()

                    elapsed = time.time() - start_time
                    logger.info(f"Saved {len(records_to_save)} trade records in {elapsed:.3f}s")

        except Exception as e:
            logger.error(f"Failed to save trade data: {e}")
            # Re-add records for retry
            for record in reversed(records_to_save):
                self.data_buffer.appendleft(record)

# MultiArchiver class - THIS IS NOW PROPERLY DEFINED AT MODULE LEVEL
class MultiArchiver:
    """Manage multiple archivers"""

    def __init__(self, zmq_address: str = "tcp://localhost:5555"):
        self.zmq_address = zmq_address
        self.archivers: List[ZMQConsumer] = []

    def add_bbo_archiver(self, **kwargs) -> BBOArchiver:
        """Add BBO archiver"""
        archiver = BBOArchiver(self.zmq_address, **kwargs)
        self.archivers.append(archiver)
        return archiver

    def add_trades_archiver(self, **kwargs) -> TradesArchiver:
        """Add Trades archiver"""
        archiver = TradesArchiver(self.zmq_address, **kwargs)
        self.archivers.append(archiver)
        return archiver

    def add_book_archiver(self, **kwargs) -> BookArchiver:
        """Add L2 Book archiver"""
        archiver = BookArchiver(self.zmq_address, **kwargs)
        self.archivers.append(archiver)
        return archiver

    async def start_all(self) -> None:
        """Start all archivers"""
        tasks = []
        for archiver in self.archivers:
            task = asyncio.create_task(archiver.start())
            tasks.append(task)

        logger.info(f"Starting {len(self.archivers)} archivers")

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in archivers: {e}")
        finally:
            # Stop all archivers
            for archiver in self.archivers:
                await archiver.stop()

# Main runner functions
async def run_all_archivers():
    """
    Run selected archivers based on environment variable configuration only
    """

    # Check environment variables
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')

    if not mysql_user or not mysql_password:
        print("ERROR: MySQL credentials not found!")
        print("Create .env file with:")
        print("  MYSQL_USER=your_username")
        print("  MYSQL_PASSWORD=your_password")
        return

    # Get archiver configuration from environment variables only
    env_bbo = os.getenv('ARCHIVE_BBO', 'true').lower()
    env_trades = os.getenv('ARCHIVE_TRADES', 'true').lower()
    env_book = os.getenv('ARCHIVE_BOOK', 'true').lower()

    # Convert to boolean
    enable_bbo = env_bbo in ['true', '1', 'yes', 'on']
    enable_trades = env_trades in ['true', '1', 'yes', 'on']
    enable_book = env_book in ['true', '1', 'yes', 'on']

    # Build list of enabled data types
    enabled_types = []
    if enable_bbo:
        enabled_types.append("BBO")
    if enable_trades:
        enabled_types.append("Trades")
    if enable_book:
        enabled_types.append("L2 Book")

    if not enabled_types:
        print("ERROR: No data types enabled for archiving!")
        print("Set environment variables in .env file:")
        print("  ARCHIVE_BBO=true")
        print("  ARCHIVE_TRADES=true")
        print("  ARCHIVE_BOOK=true")
        return

    print(f"Starting Archivers for user: {mysql_user}")
    print("Consuming from ZeroMQ at tcp://localhost:5555")
    print(f"Archiving: {' + '.join(enabled_types)} data")
    print("Configuration from environment variables:")
    print(f"  ARCHIVE_BBO={env_bbo}")
    print(f"  ARCHIVE_TRADES={env_trades}")
    print(f"  ARCHIVE_BOOK={env_book}")
    print("Make sure hl_streams.py is running first!")
    print("Ctrl+C to stop")

    # Create MultiArchiver with selected components
    multi = MultiArchiver()

    if enable_bbo:
        multi.add_bbo_archiver()
        logger.info("Added BBO archiver")

    if enable_trades:
        multi.add_trades_archiver()
        logger.info("Added Trades archiver")

    if enable_book:
        multi.add_book_archiver()
        logger.info("Added L2 Book archiver")

    try:
        await multi.start_all()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Multi-archiver error: {e}")

async def run_bbo_archiver():
    """Run BBO archiver standalone (legacy function)"""

    # Check environment variables
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')

    if not mysql_user or not mysql_password:
        print("ERROR: MySQL credentials not found!")
        print("Create .env file with:")
        print("  MYSQL_USER=your_username")
        print("  MYSQL_PASSWORD=your_password")
        return

    print(f"Starting BBO Archiver for user: {mysql_user}")
    print("Consuming from ZeroMQ at tcp://localhost:5555")
    print("Make sure hl_streams.py is running first!")
    print("Ctrl+C to stop")

    archiver = BBOArchiver()

    try:
        await archiver.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Archiver error: {e}")
    finally:
        await archiver.stop()

if __name__ == "__main__":
    asyncio.run(run_all_archivers())