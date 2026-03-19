"""
Site-specific extractors for different survey platforms.
Each extractor inherits from BaseExtractor and implements site-specific logic.
"""

# Import all extractors so they're available when the package is imported
from .topsurveys_extractor import TopSurveysExtractor
from .quickrewards_extractor import QuickRewardsExtractor

# Add new extractors here as they're created
# from .surveyjunkie_extractor import SurveyJunkieExtractor
# from .pinecone_extractor import PineconeExtractor

__all__ = [
    'TopSurveysExtractor',
    'QuickRewardsExtractor',
    # Add new extractors here
]