"""
FILE: ui/components/workflow_analyzer_ui.py
Streamlit UI component for displaying workflow analysis
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List
from .workflow_analyzer import WorkflowAnalyzer


class WorkflowAnalyzerUI:
    """Streamlit UI for workflow analysis display."""

    @staticmethod
    def render_analysis(workflow_data: Dict[str, Any], show_raw: bool = False):
        """
        Render complete workflow analysis in Streamlit.

        Args:
            workflow_data: Workflow JSON data
            show_raw: Whether to show raw analysis data
        """
        analyzer = WorkflowAnalyzer(workflow_data)
        analysis = analyzer.analyze()

        # Summary at the top
        WorkflowAnalyzerUI._render_summary(analysis)

        # Create tabs for different analyses
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "⏱️ Delays",
            "🖱️ Click Elements",
            "⌨️ Press Keys",
            "📸 Screenshots",
            "📊 Full Report"
        ])

        with tab1:
            WorkflowAnalyzerUI._render_delays(analysis["delays"])

        with tab2:
            WorkflowAnalyzerUI._render_click_elements(analysis["click_elements"])

        with tab3:
            WorkflowAnalyzerUI._render_press_keys(analysis["press_keys"])

        with tab4:
            WorkflowAnalyzerUI._render_screenshots(analysis["screenshots"])

        with tab5:
            WorkflowAnalyzerUI._render_full_report(analyzer)

        if show_raw:
            with st.expander("🔍 Raw Analysis Data (JSON)"):
                st.json(analysis)

    @staticmethod
    def _render_summary(analysis: Dict[str, Any]):
        """Render summary statistics."""
        st.subheader("📋 Workflow Analysis Summary")

        summary = analysis["summary"]

        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Delays",
                summary["total_delays"],
                delta=f"{summary['total_delay_time']/1000:.1f}s total"
            )

        with col2:
            st.metric(
                "Click Elements",
                summary["total_clicks"],
                delta=f"{summary['avg_click_timeout']:.1f}s avg timeout"
            )

        with col3:
            st.metric(
                "Press-Key Actions",
                summary["total_press_keys"],
                delta=f"{summary['avg_press_time']:.1f}s avg time"
            )

        with col4:
            st.metric(
                "Screenshots",
                summary["total_screenshots"]
            )

        st.markdown("---")

    @staticmethod
    def _render_delays(delays: List[Dict[str, Any]]):
        """Render delays analysis."""
        st.subheader("⏱️ Delay Analysis")

        if not delays:
            st.info("No delays found in this workflow")
            return

        st.success(f"Found {len(delays)} delay(s)")

        # Create DataFrame
        delay_data = []
        for i, delay in enumerate(delays, 1):
            delay_data.append({
                "#": i,
                "Group": delay["group_name"],
                "Duration (ms)": delay["delay_ms"],
                "Duration (s)": f"{delay['delay_seconds']:.2f}",
                "Disabled": "✅ Yes" if delay["is_disabled"] else "❌ No",
                "Description": delay["description"] or "N/A"
            })

        df = pd.DataFrame(delay_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Total delay time
        total_delay = sum(d["delay_ms"] for d in delays)
        st.info(f"**Total Delay Time:** {total_delay}ms ({total_delay/1000:.2f} seconds)")

        # Show individual delay cards
        with st.expander("📄 View Detailed Delay Information"):
            for i, delay in enumerate(delays, 1):
                st.markdown(f"**Delay #{i}**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"- **Group:** {delay['group_name']}")
                    st.write(f"- **Duration:** {delay['delay_ms']}ms ({delay['delay_seconds']}s)")
                with col2:
                    st.write(f"- **Item ID:** {delay['item_id']}")
                    st.write(f"- **Disabled:** {'Yes' if delay['is_disabled'] else 'No'}")
                if delay['description']:
                    st.write(f"- **Description:** {delay['description']}")
                st.markdown("---")

    @staticmethod
    def _render_click_elements(clicks: List[Dict[str, Any]]):
        """Render click elements analysis."""
        st.subheader("🖱️ Click Elements Analysis")

        if not clicks:
            st.info("No click elements found in this workflow")
            return

        st.success(f"Found {len(clicks)} click element(s)")

        # Create DataFrame
        click_data = []
        for i, click in enumerate(clicks, 1):
            click_data.append({
                "#": i,
                "Group": click["group_name"],
                "Selector": click["selector"][:50] + "..." if len(click["selector"]) > 50 else click["selector"],
                "Find By": click["find_by"],
                "Wait?": "✅" if click["wait_for_selector"] else "❌",
                "Timeout (ms)": click["selector_timeout_ms"],
                "Timeout (s)": f"{click['selector_timeout_seconds']:.2f}",
                "Disabled": "✅" if click["is_disabled"] else "❌"
            })

        df = pd.DataFrame(click_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Statistics
        total_timeout = sum(c["selector_timeout_ms"] for c in clicks)
        avg_timeout = total_timeout / len(clicks) if clicks else 0
        st.info(f"**Total Timeout Time:** {total_timeout}ms ({total_timeout/1000:.2f}s) | **Average:** {avg_timeout}ms ({avg_timeout/1000:.2f}s)")

        # Show individual click cards
        with st.expander("📄 View Detailed Click Information"):
            for i, click in enumerate(clicks, 1):
                st.markdown(f"**Click Element #{i}**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"- **Group:** {click['group_name']}")
                    st.write(f"- **Selector:** `{click['selector']}`")
                    st.write(f"- **Find By:** {click['find_by']}")
                    st.write(f"- **Wait for Selector:** {'Yes' if click['wait_for_selector'] else 'No'}")
                with col2:
                    st.write(f"- **Timeout:** {click['selector_timeout_ms']}ms ({click['selector_timeout_seconds']}s)")
                    st.write(f"- **Multiple:** {'Yes' if click['multiple'] else 'No'}")
                    st.write(f"- **Mark Element:** {'Yes' if click['mark_element'] else 'No'}")
                    st.write(f"- **Disabled:** {'Yes' if click['is_disabled'] else 'No'}")
                if click['description']:
                    st.write(f"- **Description:** {click['description']}")
                st.markdown("---")

    @staticmethod
    def _render_press_keys(press_keys: List[Dict[str, Any]]):
        """Render press-key analysis."""
        st.subheader("⌨️ Press-Key Actions Analysis")

        if not press_keys:
            st.info("No press-key actions found in this workflow")
            return

        st.success(f"Found {len(press_keys)} press-key action(s)")

        # Create DataFrame
        press_data = []
        for i, press in enumerate(press_keys, 1):
            press_data.append({
                "#": i,
                "Group": press["group_name"],
                "Keys": press["keys"][:30] + "..." if len(press["keys"]) > 30 else press["keys"],
                "Action": press["action"],
                "Press Time (ms)": press["press_time_ms"],
                "Press Time (s)": f"{press['press_time_seconds']:.2f}",
                "Disabled": "✅" if press["is_disabled"] else "❌"
            })

        df = pd.DataFrame(press_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Statistics
        total_press_time = sum(p["press_time_ms"] for p in press_keys)
        avg_press_time = total_press_time / len(press_keys) if press_keys else 0
        st.info(f"**Total Press Time:** {total_press_time}ms ({total_press_time/1000:.2f}s) | **Average:** {avg_press_time}ms ({avg_press_time/1000:.2f}s)")

        # Show individual press-key cards
        with st.expander("📄 View Detailed Press-Key Information"):
            for i, press in enumerate(press_keys, 1):
                st.markdown(f"**Press-Key Action #{i}**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"- **Group:** {press['group_name']}")
                    st.write(f"- **Keys:** {press['keys']}")
                    st.write(f"- **Action:** {press['action']}")
                with col2:
                    st.write(f"- **Press Time:** {press['press_time_ms']}ms ({press['press_time_seconds']}s)")
                    st.write(f"- **Item ID:** {press['item_id']}")
                    st.write(f"- **Disabled:** {'Yes' if press['is_disabled'] else 'No'}")
                if press['description']:
                    st.write(f"- **Description:** {press['description']}")
                if press['selector']:
                    st.write(f"- **Selector:** `{press['selector']}`")
                st.markdown("---")

    @staticmethod
    def _render_screenshots(screenshots: List[Dict[str, Any]]):
        """Render screenshots analysis."""
        st.subheader("📸 Screenshots Analysis")

        if not screenshots:
            st.info("No screenshots found in this workflow")
            return

        st.success(f"Found {len(screenshots)} screenshot(s)")

        # Create DataFrame
        screenshot_data = []
        for i, screenshot in enumerate(screenshots, 1):
            screenshot_data.append({
                "#": i,
                "Group": screenshot["group_name"],
                "Type": screenshot["type"],
                "Save to PC": "✅" if screenshot["save_to_computer"] else "❌",
                "Quality": f"{screenshot['quality']}%",
                "Format": screenshot["ext"].upper(),
                "Disabled": "✅" if screenshot["is_disabled"] else "❌"
            })

        df = pd.DataFrame(screenshot_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Show individual screenshot cards
        with st.expander("📄 View Detailed Screenshot Information"):
            for i, screenshot in enumerate(screenshots, 1):
                st.markdown(f"**Screenshot #{i}**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"- **Group:** {screenshot['group_name']}")
                    st.write(f"- **Type:** {screenshot['type']}")
                    st.write(f"- **Capture Active Tab:** {'Yes' if screenshot['capture_active_tab'] else 'No'}")
                    st.write(f"- **Full Page:** {'Yes' if screenshot['full_page'] else 'No'}")
                with col2:
                    st.write(f"- **Save to Computer:** {'Yes' if screenshot['save_to_computer'] else 'No'}")
                    st.write(f"- **Quality:** {screenshot['quality']}%")
                    st.write(f"- **Format:** {screenshot['ext'].upper()}")
                    st.write(f"- **Disabled:** {'Yes' if screenshot['is_disabled'] else 'No'}")
                if screenshot['file_name']:
                    st.write(f"- **File Name:** {screenshot['file_name']}")
                if screenshot['description']:
                    st.write(f"- **Description:** {screenshot['description']}")
                st.markdown("---")

    @staticmethod
    def _render_full_report(analyzer: WorkflowAnalyzer):
        """Render full text report."""
        st.subheader("📊 Full Analysis Report")

        # Generate report
        report = analyzer.get_formatted_report()

        # Display in code block
        st.code(report, language="text")

        # Download button
        st.download_button(
            label="📥 Download Report as Text",
            data=report,
            file_name=f"workflow_analysis_{analyzer.analysis['workflow_info']['name'].replace(' ', '_')}.txt",
            mime="text/plain"
        )

        # JSON download
        json_report = analyzer.get_json_report()
        st.download_button(
            label="📥 Download Report as JSON",
            data=json_report,
            file_name=f"workflow_analysis_{analyzer.analysis['workflow_info']['name'].replace(' ', '_')}.json",
            mime="application/json"
        )


def render_workflow_analysis(workflow_data: Dict[str, Any], show_raw: bool = False):
    """
    Convenience function to render workflow analysis.

    Args:
        workflow_data: Workflow JSON data
        show_raw: Whether to show raw analysis data
    """
    WorkflowAnalyzerUI.render_analysis(workflow_data, show_raw)
