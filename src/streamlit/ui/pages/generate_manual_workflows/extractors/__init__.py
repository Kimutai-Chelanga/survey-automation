"""
Site-specific extractors for different survey platforms.
Each extractor inherits from BaseExtractor and implements site-specific logic.
"""

# Import all extractors so they're available when the package is imported
from .topsurveys_extractor import TopSurveysExtractor

# QuickRewards extractor — import only if the file exists to avoid ImportError
try:
    from .quickrewards_extractor import QuickRewardsExtractor
    _has_quickrewards = True
except ImportError:
    try:
        from .quickrewards_ectractor import QuickRewardsExtractor  # typo fallback
        _has_quickrewards = True
    except ImportError:
        _has_quickrewards = False

__all__ = ['TopSurveysExtractor']
if _has_quickrewards:
    __all__.append('QuickRewardsExtractor')