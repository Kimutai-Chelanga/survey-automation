# src/streamlit/ui/pages/generate_manual_workflows/orchestrator.py
"""
SurveySiteOrchestrator
Loads site modules from:
  extractors/         → classes ending in "Extractor"
  workflow_creators/  → classes ending in "WorkflowCreator"

Both must exist for a site to appear in the UI.
"""

import importlib.util
import inspect
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SurveySiteOrchestrator:

    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.extractors: Dict[str, Any] = {}
        self.workflow_creators: Dict[str, Any] = {}
        self._load_modules()

    # ------------------------------------------------------------------
    # Module loading
    # ------------------------------------------------------------------

    def _load_modules(self):
        base = Path(__file__).parent  # .../generate_manual_workflows/

        # Base classes live in base/ but the extractor/creator files import them as:
        #   from extraction.base_extractor import BaseExtractor
        #   from extraction.base_workflow_creator import BaseWorkflowCreator
        # So we register them under BOTH names so either works.
        base_dir = base / "base"
        for filename, module_names in [
            ("base_extractor.py",       ["extraction.base_extractor",       "genmw.base_extractor"]),
            ("base_workflow_creator.py",["extraction.base_workflow_creator", "genmw.base_workflow_creator"]),
        ]:
            filepath = base_dir / filename
            for mod_name in module_names:
                self._preload_file(filepath, mod_name)

        self._load_dir(base / "extractors",       "Extractor",       self.extractors)
        self._load_dir(base / "workflow_creators", "WorkflowCreator", self.workflow_creators)

        logger.info(
            f"Orchestrator loaded — "
            f"extractors: {list(self.extractors.keys())} | "
            f"creators:   {list(self.workflow_creators.keys())}"
        )

    def _preload_file(self, filepath: Path, module_name: str):
        """
        Register a file into sys.modules under module_name.
        This lets subclass files do `from extraction.base_extractor import ...`
        and find it regardless of the actual directory structure.
        """
        if not filepath.exists():
            logger.warning(f"Base file not found: {filepath}")
            return
        if module_name in sys.modules:
            return
        try:
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            mod  = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = mod
            spec.loader.exec_module(mod)
            logger.info(f"Pre-loaded: {module_name} ({filepath.name})")
        except Exception as e:
            logger.error(f"Failed to pre-load {module_name}: {e}", exc_info=True)

    def _load_dir(self, directory: Path, class_suffix: str, target: Dict):
        """
        Load every *.py in directory by absolute file path.
        Finds the first class ending with class_suffix, instantiates it,
        and registers it by site_name.
        """
        if not directory.exists():
            logger.warning(f"Directory not found: {directory}")
            return

        for fp in sorted(directory.glob("*.py")):
            if fp.stem.startswith("__"):
                continue

            module_name = f"_genmw_{fp.stem}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fp)
                mod  = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = mod
                spec.loader.exec_module(mod)

                for name, obj in inspect.getmembers(mod, inspect.isclass):
                    if (
                        name.endswith(class_suffix)
                        and name not in ("BaseExtractor", "BaseWorkflowCreator")
                        and obj.__module__ == module_name
                    ):
                        instance  = obj(self.db_manager)
                        site_name = instance.get_site_info().get("site_name")
                        if site_name:
                            target[site_name] = instance
                            logger.info(f"  ✓ {class_suffix}: '{site_name}' ({fp.name})")
                        break

            except Exception as exc:
                logger.error(f"  ✗ Failed to load {fp.name}: {exc}", exc_info=True)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_available_sites(self) -> List[Dict]:
        """Sites that have BOTH an extractor AND a workflow creator."""
        sites = []
        for name in sorted(set(self.extractors) & set(self.workflow_creators)):
            try:
                ei = self.extractors[name].get_site_info()
                ci = self.workflow_creators[name].get_site_info()
                sites.append({
                    "site_name":         name,
                    "extractor_version": ei.get("version", "1.0.0"),
                    "creator_version":   ci.get("version", "1.0.0"),
                    "description":       ei.get("description", ""),
                })
            except Exception as exc:
                logger.error(f"get_available_sites error for '{name}': {exc}")
        return sites

    def get_extractor_only_sites(self) -> List[str]:
        return sorted(set(self.extractors) - set(self.workflow_creators))

    def get_creator_only_sites(self) -> List[str]:
        return sorted(set(self.workflow_creators) - set(self.extractors))

    def extract_questions(self, account_id, site_id, url, profile_path,
                          site_name, **kw) -> Dict:
        if site_name not in self.extractors:
            raise ValueError(
                f"No extractor for '{site_name}'. "
                f"Available: {list(self.extractors)}"
            )
        return self.extractors[site_name].extract_questions(
            account_id=account_id, site_id=site_id,
            url=url, profile_path=profile_path, **kw
        )

    def create_workflows(self, account_id, site_id, questions, prompt,
                         site_name, **kw) -> Dict:
        if site_name not in self.workflow_creators:
            raise ValueError(
                f"No creator for '{site_name}'. "
                f"Available: {list(self.workflow_creators)}"
            )
        return self.workflow_creators[site_name].create_workflows(
            account_id=account_id, site_id=site_id,
            questions=questions, prompt=prompt, **kw
        )