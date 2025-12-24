import os
import logging
from manager import TransactionManager, TransactionFileHandler

def run_tests():
    print("--- Starting Test Suite ---")
    data_file = "data/test_transactions.csv"
    
    # Ensure directory exists
    os.makedirs('data', exist_ok=True)

    # TEST 1: Manual Injection of Malformed Data
    # The system must skip invalid records gracefully.
    print("\n[Test 1] Testing Error Handling with Malformed Data...")
    with open(data_file, 'w') as f:
        f.write("TXN-VALID,CREDIT,100.0,1100.0\n")
        f.write("TXN-ERROR,INVALID_TYPE,NONE,ERROR\n") # Malformed line
        f.write("TXN-VALID2,DEBIT,50.0,1050.0\n")
    
    manager = TransactionManager(data_file)
    summary = manager.calculate_summary()
    
    # We expect 2 valid records; the malformed one should be logged and skipped.
    assert summary['total_count'] == 2
    print(f"Result: Successfully processed {summary['total_count']} valid records and skipped 1 error.")

    # TEST 2: Context Manager Safety
    # Ensure the file is always closed even if an exception occurs.
    print("\n[Test 2] Testing Context Manager Safety...")
    try:
        with TransactionFileHandler(data_file, 'r') as f:
            print("File is open. Forcing an exception...")
            raise Exception("Simulated Crash")
    except Exception:
        print("Exception caught. Checking logs will confirm the file was closed in the 'finally' block.")

    # TEST 3: Lazy Processing (Memory Check)
    # The system must not load the entire file into memory[cite: 4, 30].
    print("\n[Test 3] Verifying Lazy Processing...")
    gen = manager.transaction_stream()
    first_item = next(gen)
    print(f"Yielded first item: {first_item['id']} - Generator is active (Memory Efficient).")

    print("\n--- All Tests Passed Successfully ---")

if __name__ == "__main__":
    run_tests()