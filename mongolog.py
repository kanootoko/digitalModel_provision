from pymongo import MongoClient, ASCENDING
from typing import Dict, Any, Literal
import logging
import datetime

class MongoHandler(logging.Handler):
    def __init__(self, conn_string: str, component_name: str, level: Literal['CRITICAL', 'ERROR', 'WARNING', 'SUCCESS', 'INFO', 'DEBUG', 'TRACE']):
        super().__init__(level)
        self.component = component_name
        self.client = MongoClient(conn_string)
        self.db = self.client.get_database('logs')
        self.logs = self.db.get_collection('logs')
        # self.logs.ensure_index([("timestamp", ASCENDING)])

    def emit(self, record: logging.LogRecord) -> None:
        entry: Dict[str, Any] = {
            'timestamp': datetime.datetime.utcnow(),
            'component': self.component,
            'level': record.levelname,
            'message': record.message,
            'user': record.user,        # type: ignore 
            'method': record.method,    # type: ignore
            'handler': record.handler,  # type: ignore
            'endpoint': record.endpoint # type: ignore
        }
        if isinstance(record.args, dict):
            entry.update(record.args)
        self.logs.insert_one(entry)