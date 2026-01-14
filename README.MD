Email warmup utilities and movers. Each script is documented in `docs/` with the SQL it uses, inputs, and expected behavior.

Quick start
```bash
source venv/bin/activate
```

send_warmup.py
- Purpose: send warmup emails using recent SMTP log content to seed mailboxes.
- Inputs: `seed_list.txt`, `email_to_warmup.txt`.
- Env: `.env` with `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, plus `WARMUP_SAME_SENDER_COUNT`, `WARMUP_WARMUP_SENDER_COUNT`, `PARALLEL_PROCESSES`.
- ClickHouse: `clickhouse_config_smtp_log` (HTTP) in `utils.py`.
- Run:
```bash
python send_warmup.py
```

inbox_mover.py
- Purpose: move inbound junk messages back to INBOX for known accounts, logging processed items.
- Inputs: `seed_list.csv` (email, ee_account_id).
- Log: `inbox_log.csv` (auto-created).
- Env: `PARALLEL_PROCESSES`.
- ClickHouse: `clickhouse_config_spam_tests` (TCP) in `utils.py`.
- Run:
```bash
python inbox_mover.py
```

Other scripts
- send_from_burned.py — send warmup emails from poor-reputation mailboxes.
- send_spam.py — send emails to seed recipients from mailboxes that need to be marked as spam. This is needed to activate spam filters.
- spam_mover.py — move inbox spam messages to Junk.

See `docs/*.md` for detailed prompts on how each script works and how to recreate them.
