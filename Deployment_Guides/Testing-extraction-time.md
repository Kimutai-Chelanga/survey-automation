# Time Window Testing Guide

## Quick Start - How to Test Your Time Windows

### Option 1: Manual Trigger (Fastest)
**Best for:** Quick validation that DAG runs successfully
**Time:** Instant

1. Go to Airflow UI
2. Find `extract_links_weekly` DAG
3. Click the ▶️ Play button
4. DAG runs immediately, bypassing ALL time checks

**When to use:** Verify the DAG works, cookies are synced, Chrome starts, etc.

---

### Option 2: Testing Mode with Time Simulation (Recommended)
**Best for:** Testing that time windows work correctly without waiting
**Time:** Minutes instead of hours

#### Step-by-Step:

1. **Upload the Testing Files**
   - Copy `extract_links_weekly_TESTING.py` to your Airflow `dags/` folder
   - Copy `ui_extraction_processing_config_TESTING.py` to your Streamlit UI location

2. **Enable Testing Mode in UI**
   - Go to Streamlit
   - Navigate to: **Extraction Config → 🧪 Testing Mode tab**
   - Check "Enable Testing Mode"
   - Set time offset (see examples below)
   - Click "Save Testing Configuration"

3. **Trigger the Testing DAG**
   - Go to Airflow UI
   - Find `extract_links_weekly_TESTING` DAG
   - Trigger it (manual or scheduled)
   - Watch the logs - it will show both real time and simulated time

---

## Testing Scenarios

### Scenario 1: Test Morning Window (05:00-09:00)
**Current time:** 16:02
**Want to test:** Morning window at 05:00

```
1. Set time offset: Calculate minutes until 05:00 tomorrow
   - If it's 16:02, you need: (24 - 16) * 60 + (60 - 2) + 5*60 = ~778 minutes
   - OR use a simpler approach: +58 minutes to test next hour

2. Alternative - Test "almost at window":
   - Set offset to make it 04:58 (2 minutes before morning window)
   - DAG will wait ~2 seconds (scaled down) then execute
```

### Scenario 2: Test "Almost at Hour" Behavior
**Current time:** 16:02
**Want to test:** Execution at exactly 17:00

```
1. Click "Next hour (-2min)" preset button
   - This sets offset to 58 minutes
   - Simulated time becomes 17:00

2. Trigger DAG
   - It thinks it's 17:00
   - If 17:00 is in your time window, it runs immediately
   - If not, it waits for next window (but scaled down)
```

### Scenario 3: Test Wait-Until-Window Behavior
**Want to test:** DAG waiting for next window

```
1. Set time offset to be OUTSIDE your windows
   - Example: If windows are 05:00-09:00 and 19:00-23:00
   - Set offset to make it 10:00 (between windows)

2. Trigger DAG
   - It will calculate wait time to 19:00
   - In testing mode: 9 hours wait = 9 minutes scaled wait
   - Watch logs to see countdown
```

---

## Understanding the Testing DAG Logs

When testing mode is active, you'll see:

```
🧪 TESTING MODE: Time offset = +58 minutes
🧪 ACTUAL TIME: 16:02:34
🧪 SIMULATED TIME: 17:00:34 (+58 min)

████████████████████████████████████████████████████████████
EXTRACTION RUN CONFIGURATION
████████████████████████████████████████████████████████████
🧪 TESTING MODE ACTIVE:
  Actual time: 2025-02-01 16:02:34 EAT
  Simulated time: 2025-02-01 17:00:34 EAT (+58 min)
```

---

## Time Offset Cheat Sheet

| Current Time | Want to Test | Offset Needed |
|--------------|--------------|---------------|
| 16:02 | Next hour (17:00) | +58 minutes |
| 16:02 | In 2 hours (18:00) | +118 minutes |
| 16:02 | Morning (05:00 next day) | +778 minutes |
| Any time | +30 min | +30 |
| Any time | +1 hour | +60 |

**Quick Formula:**
- To next hour: `60 - current_minutes - 2` (the -2 gives you a buffer)
- To specific future hour: Calculate total minutes difference

---

## Wait Time Scaling

In testing mode, wait times are automatically scaled down:

| Real Wait Time | Scaled Testing Wait |
|----------------|---------------------|
| 1 hour | 1 minute |
| 30 minutes | 30 seconds |
| 9 hours | 9 minutes |
| 12 hours | 12 minutes |

This lets you test overnight scenarios in minutes!

---

## Common Testing Workflows

### Test 1: Verify Time Windows Work
```
1. Set morning window: 05:00-09:00
2. Enable testing mode
3. Set offset to make it 06:00 (inside morning window)
4. Trigger DAG → Should run immediately
5. Set offset to make it 10:00 (outside window)
6. Trigger DAG → Should wait for evening window (19:00)
7. Watch scaled countdown in logs
```

### Test 2: Verify Hourly Triggers Work
```
1. Configure extraction at 08:00, 17:00, 20:00
2. Enable testing mode
3. Set offset to make it 07:58
4. Trigger DAG → Should wait ~2 seconds then run
5. Check logs confirm it executed at simulated 08:00
```

### Test 3: Test Cross-Window Behavior
```
1. Set offset to make it 08:55 (end of morning window)
2. Trigger DAG → Runs in morning window
3. Set offset to make it 09:05 (just past morning)
4. Trigger DAG → Should wait for evening window
```

---

## Troubleshooting

### "Time offset not working"
- Make sure you clicked "Save Testing Configuration"
- Refresh the Airflow DAG (it reads from MongoDB each run)
- Check MongoDB to confirm settings saved:
  ```javascript
  db.settings.find({category: 'system'})
  ```

### "DAG still waiting real time"
- Using the TESTING DAG? (`extract_links_weekly_TESTING`)
- Regular DAG ignores testing mode
- Check DAG logs for "🧪 TESTING MODE" message

### "Simulated time doesn't match my offset"
- Offset is applied to current time when DAG starts
- If you trigger at 16:05 with +55 offset, you get 17:00
- If you trigger at 16:10 with same offset, you get 17:05

---

## Best Practices

✅ **DO:**
- Test with small offsets first (+5, +10 minutes)
- Watch the logs to see simulated time
- Use preset buttons for common scenarios
- Disable testing mode when done

❌ **DON'T:**
- Use testing mode in production
- Forget to save testing configuration
- Mix up testing DAG and production DAG
- Set offset so large it crosses multiple days (keep under 24 hours)

---

## Production Readiness

Before going to production:

1. ✅ Test all time windows with offsets
2. ✅ Verify wait logic works
3. ✅ Test manual triggers bypass checks
4. ✅ **Disable testing mode** ⚠️
5. ✅ Switch back to production DAG (`extract_links_weekly`)
6. ✅ Verify MongoDB settings are correct

---

## Advanced: Testing Without UI

If you need to set testing mode programmatically:

```python
from pymongo import MongoClient

client = MongoClient('mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')
db = client['messages_db']

# Get current settings
settings = db.settings.find_one({'category': 'system'})

# Add testing config
settings['settings']['testing_config'] = {
    'testing_mode_enabled': True,
    'time_offset_minutes': 58  # +58 minutes
}

# Save
db.settings.update_one(
    {'category': 'system'},
    {'$set': {'settings': settings['settings']}}
)
```

---

## Summary

**For quick tests:** Use manual trigger
**For time window validation:** Use testing mode with time offsets
**For production:** Disable testing mode and use real DAG

The testing mode lets you compress hours of waiting into minutes of testing! 🚀
