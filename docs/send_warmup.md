# send_warmup.py

Goal: send warmup emails using recent SMTP log content to seed mailboxes, mixing sends from the original message sender and from other warmup accounts.

Data sources
- Messages (ClickHouse HTTP, smtp_logs):  
  ```sql
  SELECT sender_email, subject, email_body as plain_text, email_html
  FROM smtp_logs
  WHERE 1=1
      AND subject LIKE 'Simple fix for%deliverability'
      AND is_warmup = False
      AND is_followup = False
      AND is_spamtest = False
      AND date(ts) >= today() - INTERVAL '3 day'
      AND is_sent = True
  ```
- Recipients: `seed_list.txt` (one email per line).
- Warmup sender pool: `email_to_warmup.txt` (one sender per line).

Behavior
- Fetch messages with the above query; skip if none.
- Read seed recipients and warmup senders from text files.
- For each fetched message:
  - Queue `same_sender_count` tasks from the message sender to random seeds.
  - Queue `warmup_sender_count` tasks from random warmup senders to random seeds.
- Send tasks in parallel using a shared SMTP connection per worker (`create_smtp_connection`), via `send_email_via_connection` with `message_type="warmup"`. Each task retries once with a fresh connection on failure.

Config and files
- SMTP connection from `.env` via `utils` (`SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`).
- ClickHouse config: `clickhouse_config_smtp_log` (HTTP).
- Env: `WARMUP_SAME_SENDER_COUNT`, `WARMUP_WARMUP_SENDER_COUNT`, `PARALLEL_PROCESSES`.
- Input: `seed_list.txt`, `email_to_warmup.txt`.

Run
```bash
python send_warmup.py
```
