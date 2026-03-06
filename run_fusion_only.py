import functions as func
from correlate import run_correlation
import inventory_manager

if __name__ == "__main__":
    # Initialize logger for this manual step
    func.perf_logger.start_run()
    print("Running Fusion Step only...")
    run_correlation()
    inventory_manager.rebuild_inventory()
    func.perf_logger.stop_run()
