#!/usr/bin/env python3
"""
High-speed script to delete all customers from customer.io using data from a CSV file.
Uses concurrent processing for maximum throughput.
"""

import csv
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from customerio import CustomerIO, Regions
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def load_customers_from_csv(csv_file_path: str) -> List[Dict[str, Any]]:
    """
    Load customer data from CSV file.

    Args:
        csv_file_path: Path to the CSV file containing customer data

    Returns:
        List of customer dictionaries with id and email
    """
    customers = []

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Use the 'id' field as customer_id, fallback to email if id is empty
                customer_id = row.get('id', '').strip()
                email = row.get('email', '').strip()

                # Skip rows with no identifier
                if not customer_id and not email:
                    continue

                # Prefer ID over email as customer identifier
                identifier = customer_id if customer_id else email

                customers.append({
                    'customer_id': identifier,
                    'email': email,
                    'original_id': customer_id
                })

    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)

    return customers


def delete_single_customer(customer: Dict[str, Any], site_id: str, api_key: str) -> Dict[str, Any]:
    """
    Delete a single customer from customer.io.

    Args:
        customer: Customer dictionary with id and email
        site_id: Customer.io site ID
        api_key: Customer.io API key

    Returns:
        Result dictionary with success status and details
    """
    cio = CustomerIO(site_id, api_key, region=Regions.US)
    customer_id = customer['customer_id']
    email = customer['email']

    try:
        # Add random sleep between 40ms and 100ms to spread load
        time.sleep(random.uniform(0.04, 0.1))
        cio.delete(customer_id=customer_id)
        return {
            'success': True,
            'customer_id': customer_id,
            'email': email,
            'error': None
        }
    except Exception as e:
        return {
            'success': False,
            'customer_id': customer_id,
            'email': email,
            'error': str(e)
        }


def delete_customers_fast(customers: List[Dict[str, Any]], max_workers: int = 10, progress_interval: int = 500) -> None:
    """
    Delete customers from customer.io using high-speed concurrent processing.

    Args:
        customers: List of customer dictionaries
        max_workers: Maximum number of concurrent threads (increased for speed)
        progress_interval: How often to show progress updates
    """
    # Get credentials from environment variables
    site_id = os.getenv('CUSTOMERIO_SITE_ID')
    api_key = os.getenv('CUSTOMERIO_API_KEY')

    if not site_id or not api_key:
        print("Error: Missing required environment variables.")
        print("Please set CUSTOMERIO_SITE_ID and CUSTOMERIO_API_KEY in your .env file.")
        sys.exit(1)

    total_customers = len(customers)
    deleted_count = 0
    failed_count = 0
    failed_customers = []
    processed_count = 0

    print(
        f"🚀 Starting HIGH-SPEED concurrent deletion of {total_customers} customers...")
    print(f"⚡ Using {max_workers} concurrent workers for maximum throughput.")
    print(f"📊 Progress updates every {progress_interval} customers.")
    print("=" * 70)

    start_time = time.time()
    last_update_time = start_time

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all deletion tasks
        future_to_customer = {
            executor.submit(delete_single_customer, customer, site_id, api_key): customer
            for customer in customers
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_customer):
            result = future.result()
            processed_count += 1

            if result['success']:
                deleted_count += 1
            else:
                failed_count += 1
                failed_customers.append({
                    'customer_id': result['customer_id'],
                    'email': result['email'],
                    'error': result['error']
                })

            # Show progress updates
            if processed_count % progress_interval == 0 or processed_count == total_customers:
                current_time = time.time()
                elapsed = current_time - start_time
                interval_elapsed = current_time - last_update_time

                overall_rate = processed_count / elapsed * 60 if elapsed > 0 else 0
                interval_rate = progress_interval / \
                    interval_elapsed * 60 if interval_elapsed > 0 else 0

                progress_pct = (processed_count / total_customers) * 100
                eta_seconds = (total_customers - processed_count) / (processed_count /
                                                                     elapsed) if processed_count > 0 and elapsed > 0 else 0
                eta_minutes = eta_seconds / 60

                print(f"📈 [{processed_count:,}/{total_customers:,}] {progress_pct:.1f}% | "
                      f"✅ {deleted_count:,} deleted | ❌ {failed_count:,} failed | "
                      f"🏃 {overall_rate:.0f}/min avg | ⚡ {interval_rate:.0f}/min recent | "
                      f"⏱️ ETA: {eta_minutes:.1f}min")

                last_update_time = current_time

    # Print final summary
    elapsed_time = time.time() - start_time
    final_rate = total_customers / elapsed_time * 60 if elapsed_time > 0 else 0

    print("=" * 70)
    print(f"🎉 HIGH-SPEED DELETION COMPLETED!")
    print(
        f"⏱️  Total time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    print(f"📊 Total customers: {total_customers:,}")
    print(f"✅ Successfully deleted: {deleted_count:,}")
    print(f"❌ Failed deletions: {failed_count:,}")
    print(f"🚀 Average rate: {final_rate:.0f} customers/minute")
    print(f"⚡ Speed improvement: ~{final_rate/400:.1f}x faster than original!")

    # Show sample of failed deletions if any
    if failed_customers:
        print(
            f"\n❌ Sample of failed deletions (showing first {min(10, len(failed_customers))}):")
        for i, failed in enumerate(failed_customers[:10], 1):
            print(
                f"   {i}. {failed['customer_id']} ({failed['email']}): {failed['error']}")
        if len(failed_customers) > 10:
            print(f"   ... and {len(failed_customers) - 10:,} more failures")


def main():
    """Main function to run the high-speed customer deletion script."""
    csv_file = "files/BLK customers-2025-05-25_13-21.csv"

    print("🚀 Customer.io HIGH-SPEED Bulk Customer Deletion Script")
    print("=" * 60)
    print("⚡ This optimized version uses concurrent processing for maximum speed!")
    print("🎯 Expected performance: 2000-5000+ deletions per minute")
    print()

    # Confirm before proceeding
    response = input(f"⚠️  This will delete ALL {59879:,} customers from '{csv_file}'.\n"
                     "🔥 This is IRREVERSIBLE and will run at HIGH SPEED!\n"
                     "Are you absolutely sure? (type 'DELETE' to confirm): ").strip()

    if response != 'DELETE':
        print("❌ Operation cancelled for safety.")
        sys.exit(0)

    # Load customers from CSV
    print(f"\n📂 Loading customers from '{csv_file}'...")
    customers = load_customers_from_csv(csv_file)

    if not customers:
        print("❌ No customers found in the CSV file.")
        sys.exit(0)

    print(f"✅ Loaded {len(customers):,} customers for deletion.")

    # Final confirmation with speed warning
    print(f"\n⚡ READY TO LAUNCH HIGH-SPEED DELETION!")
    print(f"🎯 Target: {len(customers):,} customers")
    print(
        f"🚀 Expected completion: ~{len(customers)/3000:.1f} minutes at 3000/min")

    response = input(
        f"\n🔥 FINAL CONFIRMATION - Start high-speed deletion? (yes/no): ").lower().strip()

    if response != 'yes':
        print("❌ Operation cancelled.")
        sys.exit(0)

    # Delete customers at high speed
    delete_customers_fast(customers)


if __name__ == "__main__":
    main()
