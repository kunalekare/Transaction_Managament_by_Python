import csv
import random
import logging
import os
from typing import Generator, Dict, Any, Optional

# Configure logging as per project requirements [cite: 17, 27]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/transaction_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TransactionManager")

class TransactionFileHandler:
    """Custom Context Manager for safe file operations."""
    def __init__(self, file_path: str, mode: str):
        self.file_path = file_path
        self.mode = mode
        self.file = None

    def __enter__(self):
        try:
            logger.info(f"Attempting to open {self.file_path} in '{self.mode}' mode. [cite: 17]")
            self.file = open(self.file_path, self.mode, newline='')
            return self.file
        except IOError as e:
            logger.error(f"Failed to open file: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()
            logger.info(f"File {self.file_path} successfully closed. [cite: 16, 17]")
        if exc_type:
            logger.error(f"An exception occurred: {exc_val} [cite: 16]")
        return False  # Propagate exceptions

class TransactionManager:
    """Encapsulates balance management and lazy processing."""
    
    def __init__(self, file_path: str, initial_balance: float = 1000.0):
        self.file_path = file_path
        self.current_balance = initial_balance

    def generate_records(self, num_transactions: int = 100) -> None:
        """Generates random transactions and persists them to file[cite: 9, 13]."""
        with TransactionFileHandler(self.file_path, 'w') as f:
            writer = csv.writer(f)
            for i in range(1, num_transactions + 1):
                t_id = f"TXN-{i:05}"
                # Determine type and dynamic range based on balance [cite: 10, 11]
                t_type = random.choice(['CREDIT', 'DEBIT'])
                
                if t_type == 'DEBIT':
                    # Amount must not exceed available balance [cite: 12]
                    amount = round(random.uniform(1.0, self.current_balance), 2) if self.current_balance > 1 else 0.0
                    self.current_balance -= amount
                else:
                    amount = round(random.uniform(10.0, 500.0), 2)
                    self.current_balance += amount

                # Persist to file in structured format [cite: 13]
                writer.writerow([t_id, t_type, amount, round(self.current_balance, 2)])

    def transaction_stream(self) -> Generator[Dict[str, Any], None, None]:
        """Lazy generator to read file one line at a time[cite: 18, 19, 31]."""
        with TransactionFileHandler(self.file_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    # Convert raw lines into structured dictionary [cite: 20]
                    yield {
                        'id': row[0],
                        'type': row[1],
                        'amount': float(row[2]),
                        'balance': float(row[3])
                    }
                except (IndexError, ValueError) as e:
                    # Log parsing errors and skip invalid records 
                    logger.error(f"Skipping malformed record {row}: {e}")
                    continue

    def calculate_summary(self) -> Dict[str, Any]:
        """Processes transactions lazily to compute a summary[cite: 22, 30]."""
        stats = {
            "total_count": 0, # [cite: 23]
            "total_credit": 0.0, # [cite: 24]
            "total_debit": 0.0, # [cite: 24]
            "final_balance": 0.0 # [cite: 25]
        }

        for txn in self.transaction_stream():
            stats["total_count"] += 1
            if txn['type'] == 'CREDIT':
                stats["total_credit"] += txn['amount']
            else:
                stats["total_debit"] += txn['amount']
            stats["final_balance"] = txn['balance']

        return stats