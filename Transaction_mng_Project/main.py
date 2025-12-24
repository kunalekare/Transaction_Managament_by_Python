import os
from manager import TransactionManager

def main():
    # Setup workspace
    DATA_DIR = "data"
    LOG_DIR = "logs"
    FILE_NAME = os.path.join(DATA_DIR, "transactions.csv")

    for folder in [DATA_DIR, LOG_DIR]:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Initialize the system 
    manager = TransactionManager(FILE_NAME, initial_balance=5000.0)

    # Step 1: Generate Data
    print(f"--- Generating 1,000 transactions to {FILE_NAME} ---")
    manager.generate_records(1000)

    # Step 2: Lazy Processing [cite: 19, 31]
    print("--- Processing data lazily (Memory Efficient) ---")
    summary = manager.calculate_summary()

    # Step 3: Output Results [cite: 22]
    print("\n" + "="*30)
    print("TRANSACTION SUMMARY REPORT")
    print("="*30)
    print(f"Processed Records: {summary['total_count']}")
    print(f"Total Credited:   ${summary['total_credit']:,.2f}")
    print(f"Total Debited:    ${summary['total_debit']:,.2f}")
    print(f"Final Account Bal: ${summary['final_balance']:,.2f}")
    print("="*30)

if __name__ == "__main__":
    main()