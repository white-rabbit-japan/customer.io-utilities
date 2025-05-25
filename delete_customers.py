#!/usr/bin/env python3
"""
Script to delete all customers from customer.io using data from a CSV file.
Reads customer data from CSV and deletes each customer using the customer.io API.
Optimized for high-speed concurrent processing.
"""

import csv
import os
import sys
import time
import asyncio
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


def delete_customers(customers: List[Dict[str, Any]], batch_size: int = 10, delay: float = 0.5) -> None:
    """
    Delete customers from customer.io.

    Args:
        customers: List of customer dictionaries
        batch_size: Number of customers to process before adding a delay
        delay: Delay in seconds between batches to avoid rate limiting
    """
    # Get credentials from environment variables
    site_id = os.getenv('CUSTOMERIO_SITE_ID')
    api_key = os.getenv('CUSTOMERIO_API_KEY')

    if not site_id or not api_key:
        print("Error: Missing required environment variables.")
        print("Please set CUSTOMERIO_SITE_ID and CUSTOMERIO_API_KEY in your .env file.")
        sys.exit(1)

    # Initialize Customer.io client
    cio = CustomerIO(site_id, api_key, region=Regions.US)

    total_customers = len(customers)
    deleted_count = 0
    failed_count = 0
    failed_customers = []

    print(f"Starting deletion of {total_customers} customers...")
    print(
        f"Processing in batches of {batch_size} with {delay}s delay between batches.")
    print("-" * 60)

    for i, customer in enumerate(customers, 1):
        customer_id = customer['customer_id']
        email = customer['email']

        try:
            # Delete the customer
            cio.delete(customer_id=customer_id)
            deleted_count += 1
            print(f"✓ [{i}/{total_customers}] Deleted: {customer_id} ({email})")

        except Exception as e:
            failed_count += 1
            failed_customers.append({
                'customer_id': customer_id,
                'email': email,
                'error': str(e)
            })
            print(
                f"✗ [{i}/{total_customers}] Failed: {customer_id} ({email}) - {e}")

        # Add delay between batches to avoid rate limiting
        if i % batch_size == 0 and i < total_customers:
            print(f"  Processed {i} customers, waiting {delay}s...")
            time.sleep(delay)

    # Print summary
    print("-" * 60)
    print(f"Deletion completed!")
    print(f"Total customers: {total_customers}")
    print(f"Successfully deleted: {deleted_count}")
    print(f"Failed deletions: {failed_count}")

    # Show failed deletions if any
    if failed_customers:
        print("\nFailed deletions:")
        for failed in failed_customers:
            print(
                f"  - {failed['customer_id']} ({failed['email']}): {failed['error']}")


def main():
    """Main function to run the customer deletion script."""
    csv_file = "customers-2025-05-25_12-42.csv"

    print("Customer.io Bulk Customer Deletion Script")
    print("=" * 50)

    # Confirm before proceeding
    response = input(f"This will delete ALL customers from the CSV file '{csv_file}'.\n"
                     "Are you sure you want to proceed? (yes/no): ").lower().strip()

    if response != 'yes':
        print("Operation cancelled.")
        sys.exit(0)

    # Load customers from CSV
    print(f"\nLoading customers from '{csv_file}'...")
    customers = load_customers_from_csv(csv_file)

    if not customers:
        print("No customers found in the CSV file.")
        sys.exit(0)

    print(f"Found {len(customers)} customers to delete.")

    # Final confirmation
    response = input(
        f"\nProceed with deleting {len(customers)} customers? (yes/no): ").lower().strip()

    if response != 'yes':
        print("Operation cancelled.")
        sys.exit(0)

    # Delete customers
    delete_customers(customers)


if __name__ == "__main__":
    main()
