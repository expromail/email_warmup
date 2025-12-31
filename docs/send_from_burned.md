# send_from_burned.py

Goal: send 15 warmup emails from every poor-reputation mailbox to random recipients from `seed_list.csv`, using message content sampled from recent SMTP logs.

Data sources
- Messages (ClickHouse HTTP, smtp_logs):  
  ```sql
  SELECT
      subject as subject,
      email_body as plain_text,
      email_html as html
  FROM smtp_logs
  WHERE 1=1
      AND is_warmup = 0
      AND is_followup = 0
      AND is_spamtest = 0
      AND date(ts) >= today() - INTERVAL 2 day
      AND is_sent = 1
      AND rand() % 1000 = 0
  LIMIT 1000;
  ```
- Burned senders (ClickHouse native, reputation_test_results):  
  ```sql
  select sender as email
  from reputation_test_results
  where 1=1
  AND date(created_at) = today() - INTERVAL 3 DAY
  AND spam_google >= 3
  AND sender NOT LIKE '%maildoso%'
  AND sender NOT LIKE '%dosomail%'
  ```
- Recipients: `seed_list.csv` column `email`.

Behavior
- Load up to 1000 candidate messages from `smtp_logs`.
- Load burned sender emails via ClickHouse TCP and skip if none found.
- Read all recipients from `seed_list.csv`.
- For each burned sender, enqueue 15 tasks; each task picks a random message and a random seed recipient.
- Send all tasks in parallel via a pooled SMTP connection using `send_email_via_connection` with `message_type="warmup"`. On failure, retry once with a new connection.

Config and files
- SMTP connection from `.env` via `utils` (`SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`).
- ClickHouse configs: `clickhouse_config_smtp_log` (HTTP) and `clickhouse_config_spam_tests` (TCP).
- Env: `PARALLEL_PROCESSES` to control pool size.
- Input: `seed_list.csv` (must have header `email`).

Run
```bash
python send_from_burned.py
```
