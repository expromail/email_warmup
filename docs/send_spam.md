# send_spam.py

Goal: send spam-test emails from a list of spam senders to seed recipients using recent SMTP log content.

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
  LIMIT 10;
  ```
- Senders: `spam_senders.txt` (one email per line).
- Recipients: `seed_list.csv` column `email`.

Behavior
- Fetch candidate messages with the query above.
- Read spam senders and seed recipients; abort if any list is empty.
- For each sender: send 10 random messages to each of up to 5 randomly selected seed recipients (50 sends per sender if at least 5 seeds are available). Messages are chosen randomly per send.
- Uses `send_email` with `message_type="spam"` for each email.

Config and files
- SMTP connection from `.env` via `utils` (`SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`).
- ClickHouse config: `clickhouse_config_smtp_log` (HTTP).
- Env: `PARALLEL_PROCESSES`.
- Inputs: `spam_senders.txt`, `seed_list.csv` (header with `email`).

Run
```bash
python send_spam.py
```
