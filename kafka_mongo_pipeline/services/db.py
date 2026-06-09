from pymongo import MongoClient
from pymongo import errors as mongo_errors
from pymongo.results import UpdateResult
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import MongoSettings
from utils import get_logger

log = get_logger(__name__)

# Retry Annotation
_RETRYABLE_ERRORS = (
    mongo_errors.ConnectionFailure,
    mongo_errors.NetworkTimeout,
    mongo_errors.AutoReconnect,
)
MONGO_RETRY = retry(
        retry=retry_if_exception_type(_RETRYABLE_ERRORS),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )


class MongoManager:
    """Manages the MongoDB connection pool."""
    def __init__(self, settings: MongoSettings) -> None:
        # MongoClient automatically maintains a built-in connection pool
        self.settings = settings
        self.client = MongoClient(
            settings.uri, 
            maxPoolSize=settings.max_pool_size, 
            minPoolSize=settings.min_pool_size,
            serverSelectionTimeoutMS=settings.server_selection_timeout_ms,
            connectTimeoutMS=settings.connect_timeout_ms,
     )
                                  
        self.db = self.client[settings.database]
        log.info("MongoDB connection pool initialized.")

    def close(self) -> None:
        self.client.close()
        log.info("MongoDB connection pool closed.")


class CustomerRepository:
    """Handles persistence for customer events."""
    def __init__(self, mongo_manager: MongoManager) -> None:
        self.collection = mongo_manager.db["customers"]


       #TODO:  FIX THE INDEXES! 
        self.collection.create_index("customer_id")
        self.collection.create_index("action")
        self.collection.create_index("country")
        self.collection.create_index("_meta.ingested_at")
        log.info(
            "mongodb indexes verified/created",
            database=mongo_manager.settings.database,
            collection=self.collection.name,
        )

    def upsert_customer(self, customer_data: dict) -> bool:
        try:
            result: UpdateResult = self._update_one_with_retry(
                {"_id": customer_data["_id"]},
                {"$set": {k: v for k, v in customer_data.items() if k != "_id"}},
                upsert=True
            )
            if result.upserted_id is None and result.matched_count == 0:
                log.warning("customer.upsert_failed", id=customer_data["_id"])
                return False
            log.debug("customer.upserted", id=result.upserted_id)
            return True
        except KeyError:
            log.error("customer.missing_id", doc=customer_data)
            return False
        except mongo_errors.PyMongoError as e:
            log.error("customer.write_failed", id=customer_data.get("_id"), error=str(e))
            return False
        
    @MONGO_RETRY  
    def _update_one_with_retry(self, filter: dict, update: dict, upsert: bool = False) -> UpdateResult:
        return self.collection.update_one(filter, update, upsert=upsert)



class OrderRepository:
    """Handles persistence for order events."""
    def __init__(self, mongo_manager: MongoManager) -> None:
        self.collection = mongo_manager.db["orders"]

    def insert_order(self, order_data: dict) -> bool:
        try:
            result = self._update_one_with_retry(
                {"_id": order_data["_id"]},
                {"$set": {k: v for k, v in order_data.items() if k != "_id"}},
                upsert=True
            )
            if result.upserted_id is None and result.matched_count == 0:
                log.warning("order.upsert_failed", id=order_data["_id"])
                return False
            log.debug("order.upserted", id=result.upserted_id)
            return True
        except KeyError:
            log.error("order.missing_id", doc=order_data)
            return False
        except mongo_errors.PyMongoError as e:
            log.error("order.write_failed", id=order_data.get("_id"), error=str(e))
            return False
        
    @MONGO_RETRY  
    def _update_one_with_retry(self, filter: dict, update: dict, upsert: bool = False) -> UpdateResult:
        return self.collection.update_one(filter, update, upsert=upsert)

    