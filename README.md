# Customer.io Bulk Customer Deletion

This repo is public.

This Python script deletes all customers from Customer.io using data from a CSV file.

## Setup

1. **Install Python dependencies:**

   ```bash
   pip3 install -r requirements.txt
   ```

2. **Configure your Customer.io credentials:**
   - Open the `.env` file
   - Replace `your_site_id_here` with your actual Customer.io Site ID
   - Replace `your_api_key_here` with your actual Customer.io API Key

   You can find these credentials in your Customer.io dashboard under Settings > API Credentials.

## Usage

1. **Ensure your CSV file is in the correct format:**
   - The script expects a CSV file named `customers-2025-05-25_12-42.csv`
   - The CSV should have columns: `id`, `email`
   - The script will use the `id` field as the customer identifier, falling back to `email` if `id` is empty

2. **Choose your deletion script:**

   **For MAXIMUM SPEED (recommended):**

   ```bash
   python3 delete_customers_fast.py
   ```

   - Uses 100 concurrent threads
   - Expected rate: 2000-5000+ deletions/minute
   - Will complete ~60k customers in 12-30 minutes instead of 2.5 hours

   **For conservative approach:**

   ```bash
   python3 delete_customers.py
   ```

   - Uses sequential processing with delays
   - Rate: ~400 deletions/minute
   - Takes about 2.5 hours for 60k customers

3. **Follow the prompts:**
   - The script will ask for confirmation before proceeding
   - It will show progress as it deletes customers
   - Failed deletions will be reported at the end

## Features

- **Batch processing:** Processes customers in batches with delays to avoid rate limiting
- **Error handling:** Continues processing even if some deletions fail
- **Progress tracking:** Shows real-time progress and summary statistics
- **Safety confirmations:** Requires explicit confirmation before deleting customers
- **Flexible identifiers:** Uses customer ID or email as the identifier

## Safety Notes

- **This operation is irreversible** - deleted customers cannot be recovered
- The script includes confirmation prompts to prevent accidental deletions
- Failed deletions are logged for review
- Rate limiting is implemented to avoid API throttling

## CSV Format

The script expects a CSV file with at least these columns:

```csv
id,email
cd457c81-b2e3-417a-a28f-cb6214e2383b,user@example.com
e3c5a6f5-aa41-4a54-bf32-64288c54ce47,another@example.com
```

## Environment Variables

- `CUSTOMERIO_SITE_ID`: Your Customer.io Site ID
- `CUSTOMERIO_API_KEY`: Your Customer.io API Key
- `CUSTOMERIO_APP_API_KEY`: (Optional) Your Customer.io App API Key

## Error Handling

The script handles various error scenarios:

- Missing or invalid CSV files
- Missing environment variables
- API errors during deletion
- Network connectivity issues

All errors are logged with details for troubleshooting.
