You're absolutely right! I apologize for the confusion. The extensions should be owned by 50000:0 (the airflow user), not airflow:airflow.

Here's the correct fix:

✅ The Correct Fix for Extension Permissions
bash
# Run this to become root inside the container
docker exec -u 0 -it airflow_scheduler bash

# Fix ownership to airflow user (UID 50000, GID 0)
chown -R 50000:0 /workspace/chrome_profiles/account_tina/Default/Extensions

# Fix permissions
chmod -R 755 /workspace/chrome_profiles/account_tina/Default/Extensions

# Exit root
exit
Verify the Fix
bash
# Check ownership
docker exec -it airflow_scheduler ls -ld /workspace/chrome_profiles/account_tina/Default/Extensions/infppggnoaenmfagbfknfkancpbljcca

# Should show:
# drwxr-xr-x ... 50000 0 ... infppggnoaenmfagbfknfkancpbljcca
Why This Happens
When you install extensions in Streamlit's Chrome session, they're created with whatever user is running Streamlit. Then when the Airflow DAG tries to use that same profile, it runs as user 50000 (airflow) and can't read the extension files because they're owned by a different user.

The Root Cause
Streamlit container runs Chrome as root or some other user
Extension gets installed → owned by that user
Airflow container runs Chrome as 50000:0 (airflow user)
Airflow's Chrome can't read the extension → extension doesn't load
The Permanent Fix
Add this to your DAG's setup_display task in local_execute.py:

python
setup_display = BashOperator(
    task_id='setup_display',
    bash_command=f'''
        # ... existing code ...

        # ✅ FIX EXTENSION OWNERSHIP for Airflow user
        echo "Fixing extension ownership for Airflow (UID 50000)..."
        for profile_dir in {CHROME_PROFILE_BASE_DIR}/account_*; do
            if [ -d "$profile_dir/Default/Extensions" ]; then
                echo "  Fixing: $profile_dir/Default/Extensions"
                chown -R 50000:0 "$profile_dir/Default/Extensions" 2>/dev/null || true
                chmod -R 755 "$profile_dir/Default/Extensions" 2>/dev/null || true
            fi
        done
        echo "  ✓ Extension ownership fixed"

        # ... rest of existing code ...
    ''',
)
This ensures that every time the DAG runs, it fixes the extension permissions before starting Chrome.

Quick Test After Fix
bash
# 1. Fix the permissions (as shown above)
docker exec -u 0 -it airflow_scheduler bash -c "chown -R 50000:0 /workspace/chrome_profiles/account_tina/Default/Extensions && chmod -R 755 /workspace/chrome_profiles/account_tina/Default/Extensions"

# 2. Trigger your DAG again
# The extension should now load!
```

You should see in the logs:
```
✓ Extension service worker found: chrome-extension://infppggnoaenmfagbfknfkancpbljcca/...
✓ Extension loaded in 3s (attempt 1)
Instead of the previous timeout error.

