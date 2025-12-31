Email warmup utilities and movers. Each script is documented in `docs/` with the SQL it uses, inputs, and expected behavior.

- send_from_burned.py — send warmup emails from poor-reputation mailboxes.
- send_warmup.py — warmup emails using historic SMTP log content.
- send_spam.py — send spam-test emails to seed recipients.
- inbox_mover.py — move inbound junk messages back to inbox.
- spam_mover.py — move inbox spam messages to Junk.

See `docs/*.md` for detailed prompts on how each script works and how to recreate them.
