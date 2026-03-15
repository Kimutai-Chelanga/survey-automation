Here is a clean Markdown version you can paste directly into your README:

````markdown
## ✅ The Correct Fix (No sudo Required)

Run this:

```bash
docker exec -u 0 -it airflow_scheduler bash
````

The `-u 0` makes you root inside the container, even if you’re not root on the host.

Then run:

```bash
chown -R 50000:0 /workspace/chrome_profiles
exit
```

Now verify:

```bash
docker exec -it airflow_scheduler ls -ld /workspace/chrome_profiles/account_tina
```

It should show:

```
50000 0
```

That fixes Chrome immediately.

```

If you'd like, I can also give you a short "Why this happens" section to include under it for future debugging.
```
