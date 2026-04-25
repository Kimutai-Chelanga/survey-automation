"""
Reusable Streamlit UI components for the Generate Manual Workflows page.
"""

import streamlit as st
import csv
import io
import os
from datetime import datetime

from .constants import SCREENSHOT_LABELS, STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR


def display_batch_details(batch_id: str, batches_state: dict, screenshot_labels: dict = None, key_suffix: str = ""):
    """Display logs and screenshots for a given batch.

    key_suffix: pass a unique string (e.g. "_latest", "_inspect") when this
    function is called more than once per Streamlit render cycle for the same
    batch_id, to avoid StreamlitDuplicateElementKey errors.
    """
    batch = batches_state.get(batch_id)
    if not batch:
        st.info("No data recorded for this batch yet.")
        return
    labels = screenshot_labels or SCREENSHOT_LABELS
    st.caption(f"🕐 {batch.get('timestamp','?')}  |  👤 {batch.get('account','?')}  |  🌐 {batch.get('site','?')}")
    shot_count = len(batch.get("screenshots", []))
    tab_logs, tab_shots = st.tabs([f"📝 Logs", f"📸 Screenshots ({shot_count})"])
    with tab_logs:
        logs = batch.get("logs", [])
        if logs:
            st.code("\n".join(logs), language="log")
            st.download_button(
                "⬇️ Download logs",
                "\n".join(logs),
                f"logs_{batch_id}.txt",
                key=f"dl_log_{batch_id}{key_suffix}_ui",
            )
        else:
            st.info("No logs stored for this batch.")
    with tab_shots:
        shots = batch.get("screenshots", [])
        if shots:
            for i, shot in enumerate(shots):
                # Support both the new dict format and legacy 3-tuple (num, path, label_key)
                if isinstance(shot, dict):
                    img_path = shot.get("path", "")
                    stage = shot.get("stage", "")
                    display_label = shot.get("label") or labels.get(stage, stage)
                else:
                    _, img_path, stage = shot
                    display_label = labels.get(stage, stage)

                st.markdown(f"**{display_label}**")
                if os.path.exists(img_path):
                    st.image(img_path, use_container_width=True)
                    with open(img_path, "rb") as f:
                        img_bytes = f.read()
                    st.download_button(
                        f"⬇️ {display_label}.png",
                        img_bytes,
                        f"ss_{batch_id}_{i}_{stage}.png",
                        mime="image/png",
                        key=f"dl_ss_{batch_id}{key_suffix}_ui_{i}",
                    )
                else:
                    st.warning(f"Screenshot file missing: {img_path}")
                st.markdown("---")
        else:
            st.info("No screenshots captured for this batch.")


def display_results(r: dict):
    """Display the results of a direct answering run."""
    if r.get("action") != "direct_answering":
        return
    st.subheader("✅ Run Results")
    if r.get("status") == "failed":
        st.error(f"❌ {r.get('error','Unknown error')}")
        if st.button("Clear", key="clr_fail_ui"):
            st.session_state.generation_results = None
            st.rerun()
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("✅ Complete", r.get("complete", 0))
    c2.metric("🟡 Passed", r.get("passed", 0))
    c3.metric("❌ Failed/DQ", r.get("failed", 0))
    c4.metric("⚠️ Error", r.get("error", 0))
    for d in r.get("details", []):
        icon = {"complete": "✅", "passed": "🟡", "failed": "❌"}.get(d["outcome"], "⚠️")
        st.write(f"{icon} Survey {d['survey_number']}: **{d['outcome']}**")
        if d.get("output_snippet"):
            with st.expander(f"Details #{d['survey_number']}"):
                st.code(d["output_snippet"])
    st.caption(
        f"Account: {r['account']['username']} | Site: {r['site']['name']} | "
        f"Model: {r.get('model','')} | Batch: {r.get('batch_id','')} | {r.get('timestamp','')}"
    )
    if st.button("Clear results", key="clr_res_ui"):
        st.session_state.generation_results = None
        st.session_state.survey_progress = []
        st.rerun()


def display_screening_results_tab(acct: dict, site: dict, batches_state: dict):
    """Render the screening results table and export controls."""
    from .db_utils import load_screening_results, update_screening_status, save_screening_note
    from .ui_components import display_batch_details

    st.subheader("🏆 Survey Attempts")
    results = load_screening_results(acct["account_id"], site["site_id"])
    if not results:
        st.info("No attempts yet.")
        return
    total = len(results)
    complete_n = sum(1 for r in results if r["status"] == STATUS_COMPLETE)
    passed_n = sum(1 for r in results if r["status"] == STATUS_PASSED)
    failed_n = sum(1 for r in results if r["status"] == STATUS_FAILED)
    error_n = sum(1 for r in results if r["status"] in (STATUS_ERROR, STATUS_PENDING))
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", total)
    c2.metric("✅ Complete", complete_n)
    c3.metric("🟡 Passed", passed_n)
    c4.metric("❌ Failed", failed_n)
    c5.metric("⚠️ Error", error_n)
    success_n = complete_n + passed_n
    if total > 0:
        st.progress(success_n / total, text=f"Success rate: {int(success_n/total*100)}% ({success_n}/{total})")
    batches = sorted({r.get("batch_id") for r in results if r.get("batch_id")})
    if batches:
        sel = st.selectbox("Filter by batch:", ["All"] + batches, key="batch_filter_ui")
        if sel != "All":
            results = [r for r in results if r.get("batch_id") == sel]
    st.markdown("---")
    for r in results:
        icon = {"complete": "✅", "passed": "🟡", "failed": "❌", "pending": "⏳", "error": "⚠️"}.get(r["status"], "❓")
        ts = r["started_at"].strftime("%Y-%m-%d %H:%M") if r.get("started_at") else "?"
        with st.expander(f"{icon} **{r.get('survey_name','?')}** — {r['status'].upper()} — {ts}", expanded=False):
            ci, ca = st.columns([3, 1])
            with ci:
                st.markdown(
                    f"**Batch:** `{r.get('batch_id','—')}`  \n"
                    f"**Started:** {ts}  \n"
                    f"**Completed:** {r['completed_at'].strftime('%Y-%m-%d %H:%M') if r.get('completed_at') else '—'}"
                )
                if r.get("notes"):
                    st.caption(r["notes"])
            with ca:
                rid = r["result_id"]
                if r["status"] != STATUS_COMPLETE:
                    if st.button("✅ Mark Complete", key=f"pass_{rid}_ui", use_container_width=True):
                        update_screening_status(rid, STATUS_COMPLETE)
                        st.rerun()
                if r["status"] != STATUS_FAILED:
                    if st.button("❌ Mark DQ", key=f"fail_{rid}_ui", use_container_width=True):
                        update_screening_status(rid, STATUS_FAILED)
                        st.rerun()
                note = st.text_input("Note:", key=f"note_{rid}_ui", placeholder="Optional…")
                if note and st.button("💾 Save", key=f"savenote_{rid}_ui", use_container_width=True):
                    save_screening_note(rid, note)
                    st.rerun()
                if r.get("batch_id") and r["batch_id"] in batches_state:
                    if st.button("📋 View batch", key=f"vb_{rid}_ui", use_container_width=True):
                        st.session_state.selected_batch_for_details = r["batch_id"]
                        st.rerun()
    if st.session_state.get("selected_batch_for_details"):
        bid = st.session_state.selected_batch_for_details
        st.markdown(f"### 📁 Batch: `{bid}`")
        # Use "_selected" suffix — this is a third distinct call site
        display_batch_details(bid, batches_state, key_suffix="_selected")
        if st.button("Close batch view", key="close_batch_ui"):
            st.session_state.selected_batch_for_details = None
            st.rerun()
    st.markdown("---")
    if st.button("📥 Export CSV", key="exp_csv_ui"):
        buf = io.StringIO()
        w = csv.DictWriter(
            buf,
            fieldnames=["survey_name", "status", "started_at", "completed_at", "batch_id", "notes"],
        )
        w.writeheader()
        for r in results:
            w.writerow(
                {
                    "survey_name": r.get("survey_name", ""),
                    "status": r.get("status", ""),
                    "started_at": str(r.get("started_at", "")),
                    "completed_at": str(r.get("completed_at", "")),
                    "batch_id": r.get("batch_id", ""),
                    "notes": r.get("notes", ""),
                }
            )
        st.download_button(
            "⬇️ Download CSV",
            buf.getvalue(),
            f"screening_{acct['username']}_{site['site_name'].replace(' ','_')}.csv",
            mime="text/csv",
            key="dl_csv_ui",
        )


def display_cookie_status(acct: dict):
    """Render cookie status panel and management controls."""
    from .cookie_utils import get_all_cookie_records, delete_cookies_from_db, save_cookies_to_db
    import json

    st.subheader("🍪 Google Session Cookies")
    records = get_all_cookie_records(acct["account_id"])
    google_record = next((r for r in records if "google" in r["domain"].lower()), None)
    col_status, col_actions = st.columns([3, 2])
    with col_status:
        if google_record:
            updated = google_record.get("updated_at")
            updated_str = updated.strftime("%Y-%m-%d %H:%M UTC") if updated else "unknown"
            size_kb = (google_record.get("size_bytes") or 0) / 1024
            st.success(
                f"✅ **Cookies stored** for `{google_record['domain']}`  \n"
                f"Last updated: `{updated_str}` | Size: `{size_kb:.1f} KB`"
            )
            st.caption(
                "Cookies will be injected automatically on the next run. "
                "If login fails, delete them and re-run with your password."
            )
        else:
            st.warning(
                "⚠️ **No cookies stored** for this account.  \n"
                "Enter Google credentials below and run — cookies will be saved automatically after login."
            )
    with col_actions:
        if google_record:
            if st.button("🗑️ Delete stored cookies", key=f"del_ck_{acct['account_id']}", use_container_width=True):
                delete_cookies_from_db(acct["account_id"], "google.com")
                st.success("Cookies deleted.")
                st.rerun()
        with st.expander("📋 Paste cookies manually (JSON)"):
            st.caption("Export cookies from your browser using a cookie-export extension.")
            raw = st.text_area(
                "Cookie JSON array:",
                height=120,
                key=f"manual_ck_{acct['account_id']}",
                placeholder='[{"name":"SID","value":"...","domain":".google.com",...}]',
            )
            if st.button("💾 Save pasted cookies", key=f"save_manual_ck_{acct['account_id']}", use_container_width=True):
                try:
                    parsed = json.loads(raw.strip())
                    if not isinstance(parsed, list):
                        raise ValueError("Expected a JSON array")
                    ok = save_cookies_to_db(acct["account_id"], parsed, "google.com")
                    if ok:
                        st.success(f"✅ Saved {len(parsed)} cookies.")
                        st.rerun()
                    else:
                        st.error("Failed to save cookies to DB.")
                except Exception as ex:
                    st.error(f"Invalid JSON: {ex}")