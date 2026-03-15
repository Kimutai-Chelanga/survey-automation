#!/usr/bin/env python3
"""
Upload and trigger Automa workflows via Chrome DevTools Protocol.
Each workflow is triggered sequentially, with a 2-minute interval.
Logs are stored in a timestamped file.
"""

import os
import json
import glob
import time
import requests
import websocket

CHROME_DEBUG_URL = "http://localhost:9222/json"
WORKFLOW_DIRS = [
    "/workspace/gui-scripts/workflows"  # Only this folder is used now
]
LOG_FILE = "automa_trigger_log.txt"
INTERVAL_SECONDS = 120  # 2 minutes

def get_chrome_tabs():
    try:
        resp = requests.get(CHROME_DEBUG_URL, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"❌ Failed to get Chrome targets: {e}")
        return []

def find_automa_context():
    tabs = get_chrome_tabs()
    for tab in tabs:
        if tab.get('type') == 'background_page' and 'automa' in (tab.get('title') or '').lower():
            return tab.get('webSocketDebuggerUrl')
    for tab in tabs:
        url = (tab.get('url') or '').lower()
        if 'chrome-extension' in url and 'automa' in url:
            return tab.get('webSocketDebuggerUrl')
    for tab in tabs:
        if 'automa' in (tab.get('title') or '').lower():
            return tab.get('webSocketDebuggerUrl')
    return None

def load_workflows():
    workflows = []
    for workflow_dir in WORKFLOW_DIRS:
        if not os.path.exists(workflow_dir):
            print(f"⚠️ Skipping missing directory: {workflow_dir}")
            continue
        files = glob.glob(os.path.join(workflow_dir, "*.json"))
        if not files:
            print(f"⚠️ No JSON files in: {workflow_dir}")
            continue
        dirname = os.path.basename(workflow_dir)
        for file in files:
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                fname = os.path.splitext(os.path.basename(file))[0]
                data.setdefault("id", f"{dirname}_{fname}")
                data.setdefault("name", f"{dirname.title()} - {fname}")
                ts = int(time.time() * 1000)
                data.setdefault("createdAt", ts)
                data.setdefault("updatedAt", ts)
                data.setdefault("isDisabled", False)
                data.setdefault("description", f"Imported {dirname} workflow: {fname}")
                workflows.append(data)
                print(f"  ✅ Loaded: {data['name']}")
            except Exception as e:
                print(f"  ❌ Failed parsing {file}: {e}")
    print(f"📊 Total workflows loaded: {len(workflows)}")
    return workflows

def inject_and_trigger_workflows(ws_url, workflows, variables_map=None):
    ws = websocket.create_connection(ws_url)
    wf_map = {w["id"]: w for w in workflows}
    # Upload workflows
    upload_js = f"""
        if (chrome?.storage?.local) {{
            chrome.storage.local.set({{workflows: {json.dumps(wf_map)}}}, () => {{}});
            'uploaded';
        }} else {{
            'storage_unavailable';
        }}
        
    """
    ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {"expression": upload_js}}))
    resp = ws.recv()
    print("Upload response:", resp)
    # Trigger each workflow sequentially
    for wf in workflows:
        wf_id = wf["id"]
        wf_name = wf.get("name", wf_id)
        vars_for_wf = (variables_map or {}).get(wf_id, {})
        detail = {"id": wf_id}
        if vars_for_wf:
            detail["data"] = {"variables": vars_for_wf}
        trigger_js = f"""
            new Promise((resolve, reject) => {{
                try {{
                    const timeout = setTimeout(() => reject('timeout'), 3000);
                    window.dispatchEvent(new CustomEvent('automa:execute-workflow', {{ detail: {json.dumps(detail)} }}));
                    setTimeout(() => {{ clearTimeout(timeout); resolve('dispatched'); }}, 100);
                }} catch (e) {{
                    reject(e.message);
                }}
            }});
        """
        payload = {
            "id": int(time.time() * 1000) % 10000 + 500,
            "method": "Runtime.evaluate",
            "params": {"expression": trigger_js, "awaitPromise": True}
        }
        ws.send(json.dumps(payload))
        res = json.loads(ws.recv())
        val = res.get("result", {}).get("result", {}).get("value", "")
        success = "dispatched" in val
        status = "SUCCESS" if success else "FAILURE"
        log_line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {status} triggering '{wf_name}': {val or res}\n"
        print(log_line.strip())
        with open(LOG_FILE, "a", encoding="utf-8") as logf:
            logf.write(log_line)
        time.sleep(INTERVAL_SECONDS)
    ws.close()

def main():
    print("🔄 Starting Automa workflow upload & trigger process")
    workflows = load_workflows()
    if not workflows:
        print("No workflows loaded. Exiting.")
        return
    ws_url = find_automa_context()
    if not ws_url:
        print("⚠️ Automa context not found. Please open Automa in Chrome.")
        return
    print(f"Using websocket: {ws_url[:50]}…")
    inject_and_trigger_workflows(ws_url, workflows)
    print("✅ All workflows triggered and logged.")

if __name__ == "__main__":
    main()
