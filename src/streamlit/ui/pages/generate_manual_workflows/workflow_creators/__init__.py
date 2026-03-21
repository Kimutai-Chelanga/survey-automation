"""
Site-specific workflow creators for different survey platforms.
Each workflow creator inherits from BaseWorkflowCreator and implements site-specific logic.
"""

from .topsurveys_workflow import TopSurveysWorkflowCreator

try:
    from .quickrewards_workflow import QuickRewardsWorkflowCreator
    _has_quickrewards = True
except ImportError:
    _has_quickrewards = False

__all__ = ['TopSurveysWorkflowCreator']
if _has_quickrewards:
    __all__.append('QuickRewardsWorkflowCreator')