""" Module for retrieving appnexus reporting data """

from reportloader.platforms.appnexus.puller import AppnexusPuller 
from reportloader.platforms.criteo.puller import CriteoPuller
from reportloader.platforms.google.puller import GooglePuller
from reportloader.platforms.taboola.puller import TaboolaPuller
from reportloader.platforms.teads.puller import TeadsPuller
from reportloader.platforms.smart.puller import SmartPuller
from reportloader.platforms.smaato.puller import SmaatoPuller
from reportloader.platforms.facebook.puller import FacebookPuller
from reportloader.platforms.rubicon.puller import RubiconPuller

class ReporterPuller(object):
    """  """
    
    types = {
        'appnexus': AppnexusPuller,
        'adsense': GooglePuller,
        'adx': GooglePuller,
        'criteo': CriteoPuller,
        'criteohb': CriteoPuller,
        'teads': TeadsPuller,
        'taboola': TaboolaPuller,
        'smart': SmartPuller,
        'smaato': SmaatoPuller,
        'facebook': FacebookPuller,
        'rubicon': RubiconPuller,
        }
    
    @classmethod        
    def factory(cls, type):
        """  
        """
        if cls.types[type]:
            return cls.types[type]                 
    