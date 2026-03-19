"""
Site-specific workflow creators for different survey platforms.
Each workflow creator inherits from BaseWorkflowCreator and implements site-specific logic.
"""

# Import all workflow creators so they're available when the package is imported
from .topsurveys_workflow import TopSurveysWorkflowCreator
from .quickrewards_workflow import QuickRewardsWorkflowCreator

# Add new workflow creators here as they're created
# from .surveyjunkie_workflow import SurveyJunkieWorkflowCreator
# from .pinecone_workflow import PineconeWorkflowCreator

__all__ = [
    'TopSurveysWorkflowCreator',
    'QuickRewardsWorkflowCreator',
    # Add new workflow creators here
]