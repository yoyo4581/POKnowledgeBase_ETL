from etl.pipeline_manager import ETLPipelineManager
from dotenv import load_dotenv
import argparse



load_dotenv()  # Load environment variables from .env file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline runner")
    parser.add_argument("--approve", action="store_true", help="Approve and load to production")
    parser.add_argument("--load", action="store_true", help="Resume from last batch")

    parser.add_argument("--version", type=str, required=True, help="ETL pipeline version")
    parser.add_argument("--user_id", type=str, required=True, help="User ID for approval tracking")

    parser.add_argument("--batch_size", type=int, default=20, help="Number of pathways per batch")
    args = parser.parse_args()

    # The manager will look at if there is already a batch and pick up from the last staging phase.
    etl_manager = ETLPipelineManager(version=args.version, user_id=args.user_id)
    if args.approve:
        etl_manager.approve_batch()
    elif args.load:
        if not etl_manager.is_approved():
            print("Not approved yet. Skipping production load.")
            exit(0)
        etl_manager.load_to_production()
    else:
        # The manager's run_pipeline has a stop_batch argument to enable QA check every stop_batch numbers
        etl_manager.run_pipeline(batch_size=args.batch_size)

    