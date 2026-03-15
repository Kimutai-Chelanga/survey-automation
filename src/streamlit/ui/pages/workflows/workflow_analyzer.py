"""
FILE: src/utils/workflow_analyzer.py
Utility to analyze workflow structure and extract detailed block information
"""

from typing import Dict, Any, List, Optional
import json


class WorkflowAnalyzer:
    """Analyze Automa workflow structure and extract detailed block information."""

    def __init__(self, workflow_data: Dict[str, Any]):
        """
        Initialize analyzer with workflow data.

        Args:
            workflow_data: Complete Automa workflow JSON
        """
        self.workflow_data = workflow_data
        self.analysis = {}

    def analyze(self) -> Dict[str, Any]:
        """
        Perform complete workflow analysis.

        Returns:
            Dictionary with comprehensive analysis results
        """
        self.analysis = {
            "workflow_info": self._get_workflow_info(),
            "delays": self._analyze_delays(),
            "click_elements": self._analyze_click_elements(),
            "press_keys": self._analyze_press_keys(),
            "screenshots": self._analyze_screenshots(),
            "summary": {}
        }

        # Add summary statistics
        self.analysis["summary"] = {
            "total_delays": len(self.analysis["delays"]),
            "total_delay_time": sum(d["delay_ms"] for d in self.analysis["delays"]),
            "total_clicks": len(self.analysis["click_elements"]),
            "total_press_keys": len(self.analysis["press_keys"]),
            "total_screenshots": len(self.analysis["screenshots"]),
            "avg_click_timeout": self._calculate_avg_click_timeout(),
            "avg_press_time": self._calculate_avg_press_time()
        }

        return self.analysis

    def _get_workflow_info(self) -> Dict[str, Any]:
        """Extract basic workflow information."""
        return {
            "name": self.workflow_data.get("name", "Unknown"),
            "version": self.workflow_data.get("version", "Unknown"),
            "description": self.workflow_data.get("description", ""),
            "ext_version": self.workflow_data.get("extVersion", "Unknown")
        }

    def _get_all_blocks(self) -> List[Dict[str, Any]]:
        """Extract all blocks from the workflow including nested blocks in groups."""
        all_blocks = []

        drawflow = self.workflow_data.get("drawflow", {})
        nodes = drawflow.get("nodes", [])

        for node in nodes:
            node_data = node.get("data", {})

            # Check if this is a BlockGroup
            if node.get("type") == "BlockGroup":
                # Get blocks inside the group
                blocks = node_data.get("blocks", [])
                for block in blocks:
                    block_copy = block.copy()
                    block_copy["_group_name"] = node_data.get("name", "unnamed_group")
                    block_copy["_group_id"] = node.get("id")
                    all_blocks.append(block_copy)

            # Check if this is a BlockBasic (trigger)
            elif node.get("type") == "BlockBasic":
                all_blocks.append({
                    "id": node_data.get("id", "trigger"),
                    "data": node_data,
                    "_is_trigger": True
                })

        return all_blocks

    def _analyze_delays(self) -> List[Dict[str, Any]]:
        """
        Analyze all delay blocks in the workflow.

        Returns:
            List of delay information dictionaries
        """
        delays = []
        all_blocks = self._get_all_blocks()

        for idx, block in enumerate(all_blocks):
            block_id = block.get("id")
            block_data = block.get("data", {})

            # Check if this is a delay block
            if block_id == "delay":
                delay_info = {
                    "block_index": idx,
                    "item_id": block.get("itemId", "N/A"),
                    "group_name": block.get("_group_name", "N/A"),
                    "group_id": block.get("_group_id", "N/A"),
                    "delay_ms": int(block_data.get("time", "0")),
                    "delay_seconds": int(block_data.get("time", "0")) / 1000,
                    "is_disabled": block_data.get("disableBlock", False),
                    "description": block_data.get("description", "")
                }
                delays.append(delay_info)

        return delays

    def _analyze_click_elements(self) -> List[Dict[str, Any]]:
        """
        Analyze all click event blocks in the workflow.

        Returns:
            List of click element information dictionaries
        """
        clicks = []
        all_blocks = self._get_all_blocks()

        for idx, block in enumerate(all_blocks):
            block_id = block.get("id")
            block_data = block.get("data", {})

            # Check if this is a click event block
            if block_id == "event-click":
                click_info = {
                    "block_index": idx,
                    "item_id": block.get("itemId", "N/A"),
                    "group_name": block.get("_group_name", "N/A"),
                    "group_id": block.get("_group_id", "N/A"),
                    "selector": block_data.get("selector", ""),
                    "find_by": block_data.get("findBy", "cssSelector"),
                    "wait_for_selector": block_data.get("waitForSelector", False),
                    "selector_timeout_ms": block_data.get("waitSelectorTimeout", 0),
                    "selector_timeout_seconds": block_data.get("waitSelectorTimeout", 0) / 1000,
                    "multiple": block_data.get("multiple", False),
                    "mark_element": block_data.get("markEl", False),
                    "is_disabled": block_data.get("disableBlock", False),
                    "description": block_data.get("description", ""),
                    "on_error": block_data.get("onError", {})
                }
                clicks.append(click_info)

        return clicks

    def _analyze_press_keys(self) -> List[Dict[str, Any]]:
        """
        Analyze all press-key blocks in the workflow.

        Returns:
            List of press-key information dictionaries
        """
        press_keys = []
        all_blocks = self._get_all_blocks()

        for idx, block in enumerate(all_blocks):
            block_id = block.get("id")
            block_data = block.get("data", {})

            # Check if this is a press-key block
            if block_id == "press-key":
                press_key_info = {
                    "block_index": idx,
                    "item_id": block.get("itemId", "N/A"),
                    "group_name": block.get("_group_name", "N/A"),
                    "group_id": block.get("_group_id", "N/A"),
                    "keys": block_data.get("keys", ""),
                    "keys_to_press": block_data.get("keysToPress", ""),
                    "action": block_data.get("action", "press-key"),
                    "press_time_ms": int(block_data.get("pressTime", "0")),
                    "press_time_seconds": int(block_data.get("pressTime", "0")) / 1000,
                    "selector": block_data.get("selector", ""),
                    "is_disabled": block_data.get("disableBlock", False),
                    "description": block_data.get("description", ""),
                    "on_error": block_data.get("onError", {})
                }
                press_keys.append(press_key_info)

        return press_keys

    def _analyze_screenshots(self) -> List[Dict[str, Any]]:
        """
        Analyze all screenshot blocks in the workflow.

        Returns:
            List of screenshot information dictionaries
        """
        screenshots = []
        all_blocks = self._get_all_blocks()

        for idx, block in enumerate(all_blocks):
            block_id = block.get("id")
            block_data = block.get("data", {})

            # Check if this is a screenshot block
            if block_id == "take-screenshot":
                screenshot_info = {
                    "block_index": idx,
                    "item_id": block.get("itemId", "N/A"),
                    "group_name": block.get("_group_name", "N/A"),
                    "group_id": block.get("_group_id", "N/A"),
                    "type": block_data.get("type", "fullpage"),
                    "capture_active_tab": block_data.get("captureActiveTab", False),
                    "full_page": block_data.get("fullPage", False),
                    "save_to_computer": block_data.get("saveToComputer", False),
                    "file_name": block_data.get("fileName", ""),
                    "quality": block_data.get("quality", 100),
                    "ext": block_data.get("ext", "png"),
                    "selector": block_data.get("selector", ""),
                    "is_disabled": block_data.get("disableBlock", False),
                    "description": block_data.get("description", "")
                }
                screenshots.append(screenshot_info)

        return screenshots

    def _calculate_avg_click_timeout(self) -> float:
        """Calculate average click timeout in seconds."""
        clicks = self.analysis.get("click_elements", [])
        if not clicks:
            return 0.0

        total_timeout = sum(c["selector_timeout_ms"] for c in clicks)
        return total_timeout / len(clicks) / 1000

    def _calculate_avg_press_time(self) -> float:
        """Calculate average press time in seconds."""
        press_keys = self.analysis.get("press_keys", [])
        if not press_keys:
            return 0.0

        total_press_time = sum(p["press_time_ms"] for p in press_keys)
        return total_press_time / len(press_keys) / 1000

    def get_formatted_report(self) -> str:
        """
        Get a formatted text report of the analysis.

        Returns:
            Formatted string report
        """
        if not self.analysis:
            self.analyze()

        report_lines = []

        # Workflow Info
        report_lines.append("=" * 70)
        report_lines.append("WORKFLOW ANALYSIS REPORT")
        report_lines.append("=" * 70)
        report_lines.append("")

        info = self.analysis["workflow_info"]
        report_lines.append(f"Workflow Name: {info['name']}")
        report_lines.append(f"Version: {info['version']}")
        report_lines.append(f"Extension Version: {info['ext_version']}")
        if info['description']:
            report_lines.append(f"Description: {info['description']}")
        report_lines.append("")

        # Summary
        report_lines.append("-" * 70)
        report_lines.append("SUMMARY")
        report_lines.append("-" * 70)
        summary = self.analysis["summary"]
        report_lines.append(f"Total Delays: {summary['total_delays']}")
        report_lines.append(f"Total Delay Time: {summary['total_delay_time']}ms ({summary['total_delay_time']/1000}s)")
        report_lines.append(f"Total Click Elements: {summary['total_clicks']}")
        report_lines.append(f"Total Press-Key Actions: {summary['total_press_keys']}")
        report_lines.append(f"Total Screenshots: {summary['total_screenshots']}")
        report_lines.append(f"Average Click Timeout: {summary['avg_click_timeout']:.2f}s")
        report_lines.append(f"Average Press Time: {summary['avg_press_time']:.2f}s")
        report_lines.append("")

        # Delays
        if self.analysis["delays"]:
            report_lines.append("-" * 70)
            report_lines.append("DELAYS")
            report_lines.append("-" * 70)
            for i, delay in enumerate(self.analysis["delays"], 1):
                report_lines.append(f"\nDelay #{i}:")
                report_lines.append(f"  Group: {delay['group_name']}")
                report_lines.append(f"  Duration: {delay['delay_ms']}ms ({delay['delay_seconds']}s)")
                report_lines.append(f"  Disabled: {delay['is_disabled']}")
                if delay['description']:
                    report_lines.append(f"  Description: {delay['description']}")

        # Click Elements
        if self.analysis["click_elements"]:
            report_lines.append("")
            report_lines.append("-" * 70)
            report_lines.append("CLICK ELEMENTS")
            report_lines.append("-" * 70)
            for i, click in enumerate(self.analysis["click_elements"], 1):
                report_lines.append(f"\nClick #{i}:")
                report_lines.append(f"  Group: {click['group_name']}")
                report_lines.append(f"  Selector: {click['selector']}")
                report_lines.append(f"  Find By: {click['find_by']}")
                report_lines.append(f"  Wait for Selector: {click['wait_for_selector']}")
                report_lines.append(f"  Timeout: {click['selector_timeout_ms']}ms ({click['selector_timeout_seconds']}s)")
                report_lines.append(f"  Disabled: {click['is_disabled']}")
                if click['description']:
                    report_lines.append(f"  Description: {click['description']}")

        # Press Keys
        if self.analysis["press_keys"]:
            report_lines.append("")
            report_lines.append("-" * 70)
            report_lines.append("PRESS-KEY ACTIONS")
            report_lines.append("-" * 70)
            for i, press in enumerate(self.analysis["press_keys"], 1):
                report_lines.append(f"\nPress-Key #{i}:")
                report_lines.append(f"  Group: {press['group_name']}")
                report_lines.append(f"  Keys: {press['keys']}")
                report_lines.append(f"  Action: {press['action']}")
                report_lines.append(f"  Press Time: {press['press_time_ms']}ms ({press['press_time_seconds']}s)")
                report_lines.append(f"  Disabled: {press['is_disabled']}")
                if press['description']:
                    report_lines.append(f"  Description: {press['description']}")

        # Screenshots
        if self.analysis["screenshots"]:
            report_lines.append("")
            report_lines.append("-" * 70)
            report_lines.append("SCREENSHOTS")
            report_lines.append("-" * 70)
            for i, screenshot in enumerate(self.analysis["screenshots"], 1):
                report_lines.append(f"\nScreenshot #{i}:")
                report_lines.append(f"  Group: {screenshot['group_name']}")
                report_lines.append(f"  Type: {screenshot['type']}")
                report_lines.append(f"  Save to Computer: {screenshot['save_to_computer']}")
                report_lines.append(f"  Quality: {screenshot['quality']}%")
                report_lines.append(f"  Disabled: {screenshot['is_disabled']}")

        report_lines.append("")
        report_lines.append("=" * 70)

        return "\n".join(report_lines)

    def get_json_report(self) -> str:
        """
        Get analysis as formatted JSON string.

        Returns:
            JSON string
        """
        if not self.analysis:
            self.analyze()

        return json.dumps(self.analysis, indent=2)


def analyze_workflow_file(file_path: str) -> WorkflowAnalyzer:
    """
    Analyze a workflow from a JSON file.

    Args:
        file_path: Path to workflow JSON file

    Returns:
        WorkflowAnalyzer instance with analysis completed
    """
    with open(file_path, 'r') as f:
        workflow_data = json.load(f)

    analyzer = WorkflowAnalyzer(workflow_data)
    analyzer.analyze()
    return analyzer


def analyze_workflow_json(json_string: str) -> WorkflowAnalyzer:
    """
    Analyze a workflow from a JSON string.

    Args:
        json_string: Workflow JSON as string

    Returns:
        WorkflowAnalyzer instance with analysis completed
    """
    workflow_data = json.loads(json_string)
    analyzer = WorkflowAnalyzer(workflow_data)
    analyzer.analyze()
    return analyzer


# Example usage
if __name__ == "__main__":
    # Example workflow data
    sample_workflow = {
        "name": "Sample Workflow",
        "version": "1.0",
        "drawflow": {
            "nodes": [
                {
                    "id": "group1",
                    "type": "BlockGroup",
                    "data": {
                        "name": "Main Group",
                        "blocks": [
                            {
                                "id": "delay",
                                "itemId": "delay1",
                                "data": {
                                    "time": "5000",
                                    "disableBlock": False
                                }
                            },
                            {
                                "id": "event-click",
                                "itemId": "click1",
                                "data": {
                                    "selector": "button.submit",
                                    "waitForSelector": True,
                                    "waitSelectorTimeout": 15000,
                                    "disableBlock": False
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }

    analyzer = WorkflowAnalyzer(sample_workflow)
    analyzer.analyze()
    print(analyzer.get_formatted_report())
