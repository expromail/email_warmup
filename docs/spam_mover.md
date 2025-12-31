# spam_mover.py

Goal: move spammy inbox messages to Junk for known accounts, skipping already-processed message IDs.

Data sources
- Messages (ClickHouse TCP, email_letters_first_emails):  
  ```sql
  SELECT
      ee_id,
      arrayElement(recipients, 1) AS email
  FROM email_letters_first_emails
  WHERE 1=1
      AND date(created_at) >= today() - INTERVAL '2 day'
      AND path = '\\Inbox'
      AND id LIKE '%-smdz%'
  ```
- Account mapping: `seed_list.csv` columns `email`, `ee_account_id`.
- Processed log: `spam_log.txt` (one `ee_id` per line).

Behavior
- Fetch `(ee_id, email)` records using the query above.
- Map email -> `ee_account_id` via `seed_list.csv`; skip entries without a match.
- Skip any `ee_id` already listed in `spam_log.txt`.
- For each remaining pair, call `move_message_to_folder(account, message, "\\Junk")`. On success, append the `ee_id` to `spam_log.txt`.

Config and files
- ClickHouse config: `clickhouse_config_spam_tests` (TCP).
- Env: `PARALLEL_PROCESSES`.
- Inputs: `seed_list.csv`.
- Log: `spam_log.txt` auto-appended.

Run
```bash
python spam_mover.py
```
