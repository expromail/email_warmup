# inbox_mover.py

Goal: move inbound junk messages back to INBOX for known accounts, logging both `ee_id` and `ee_account_id` to avoid repeats.

Data sources
- Messages (ClickHouse TCP, email_letters_first_emails):  
  ```sql
  SELECT
      ee_id,
      arrayElement(recipients, 1) AS email
  FROM email_letters_first_emails
  WHERE 1=1
      AND date(created_at) >= today() - INTERVAL '1 day'
      AND path = '\\Junk'
      AND (id LIKE '<curious-gepard%' or id LIKE '%-mldz%')
  ```
- Account mapping: `seed_list.csv` columns `email`, `ee_account_id`.
- Processed log: `inbox_log.csv` (header `ee_id,ee_account_id`).

Behavior
- Fetch `(ee_id, email)` pairs using the query above.
- Load seed accounts to map email -> `ee_account_id`; skip entries without a match.
- Skip any `(ee_id, ee_account_id)` pair already present in `inbox_log.csv`.
- Shuffle pending pairs, then process in batches of 600; after each batch (if more remain) pause for 407 seconds.
- For each pair, call `move_message_to_folder(account, message, "INBOX")`. On success, append the pair to `inbox_log.csv`.

Config and files
- ClickHouse config: `clickhouse_config_spam_tests` (TCP).
- Env: `PARALLEL_PROCESSES`.
- Inputs: `seed_list.csv`.
- Log: `inbox_log.csv` auto-created with header.

Run
```bash
python inbox_mover.py
```
