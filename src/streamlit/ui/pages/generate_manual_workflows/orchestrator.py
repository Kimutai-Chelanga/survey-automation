# src/streamlit/ui/pages/generate_manual_workflows/orchestrator.py
"""
Main Orchestrator - Manages both extraction and workflow creation for multiple sites
"""

import os
import sys
import logging
import importlib
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class SurveySiteOrchestrator:
    """
    Orchestrates both extraction and workflow creation for multiple survey sites.
    Each site has its own extractor and workflow creator modules.
    """
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.extractors = {}
        self.workflow_creators = {}
        self._load_modules()
        
    def _load_modules(self):
        """Dynamically load all extractor and workflow creator modules"""
        base_dir = Path(__file__).parent
        
        # Load extractors
        extractors_dir = base_dir / 'extractors'
        self._load_from_directory(extractors_dir, 'extractors', 'Extractor', self.extractors)
        
        # Load workflow creators
        creators_dir = base_dir / 'workflow_creators'
        self._load_from_directory(creators_dir, 'workflow_creators', 'WorkflowCreator', self.workflow_creators)
        
        logger.info(f"Loaded {len(self.extractors)} extractors and {len(self.workflow_creators)} workflow creators")
    
    def _load_from_directory(self, directory: Path, module_prefix: str, 
                            class_suffix: str, target_dict: Dict):
        """Load modules from a directory"""
        if not directory.exists():
            logger.warning(f"Directory not found: {directory}")
            return
            
        # Add parent directory to path
        sys.path.insert(0, str(directory.parent))
        
        for file_path in directory.glob('*_*.py'):
            if file_path.name.startswith('__'):
                continue
                
            module_name = file_path.stem
            try:
                module = importlib.import_module(f'{module_prefix}.{module_name}')
                
                # Look for a class that ends with class_suffix
                for attr_name in dir(module):
                    if attr_name.endswith(class_suffix) and not attr_name.startswith('_'):
                        class_obj = getattr(module, attr_name)
                        instance = class_obj(self.db_manager)
                        
                        # Get site info
                        site_info = instance.get_site_info()
                        site_name = site_info.get('site_name')
                        
                        if site_name:
                            target_dict[site_name] = instance
                            logger.info(f"✅ Loaded {class_suffix.lower()} for: {site_name}")
                        break
                        
            except Exception as e:
                logger.error(f"Failed to load module {module_name}: {e}")
    
    def get_available_sites(self) -> List[Dict[str, Any]]:
        """Get list of all available survey sites with both extractors and workflow creators"""
        sites = []
        for site_name in set(self.extractors.keys()) & set(self.workflow_creators.keys()):
            try:
                extractor_info = self.extractors[site_name].get_site_info()
                creator_info = self.workflow_creators[site_name].get_site_info()
                
                sites.append({
                    'site_name': site_name,
                    'extractor_version': extractor_info.get('version', '1.0.0'),
                    'creator_version': creator_info.get('version', '1.0.0'),
                    'description': extractor_info.get('description', ''),
                    'has_extractor': True,
                    'has_creator': True
                })
            except Exception as e:
                logger.error(f"Error getting site info for {site_name}: {e}")
                
        return sites
    
    def extract_questions(self, account_id: int, site_id: int, url: str,
                          profile_path: str, site_name: str, **kwargs) -> Dict[str, Any]:
        """Extract questions using site-specific extractor"""
        if site_name not in self.extractors:
            raise ValueError(f"No extractor found for site: {site_name}")
            
        return self.extractors[site_name].extract_questions(
            account_id=account_id,
            site_id=site_id,
            url=url,
            profile_path=profile_path,
            **kwargs
        )
    
    def create_workflows(self, account_id: int, site_id: int,
                         questions: List[Dict], prompt: Optional[Dict],
                         site_name: str, **kwargs) -> Dict[str, Any]:
        """Create workflows using site-specific workflow creator"""
        if site_name not in self.workflow_creators:
            raise ValueError(f"No workflow creator found for site: {site_name}")
            
        return self.workflow_creators[site_name].create_workflows(
            account_id=account_id,
            site_id=site_id,
            questions=questions,
            prompt=prompt,
            **kwargs
        )