"""
Base classes for the generate_manual_workflows module.
All extractors and workflow creators should inherit from these base classes.
"""

from .base_extractor import BaseExtractor
from .base_workflow_creator import BaseWorkflowCreator

__all__ = ['BaseExtractor', 'BaseWorkflowCreator']