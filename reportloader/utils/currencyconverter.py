import requests
from reportloader.utils.config import Config

class CurrencyConverter():
    
    def __init__(self, data, datefield):
        self.api_uri = Config.getParam('currency', 'api_uri')
        self.__data = data
        self.__datefield = datefield
        self.__rates = {}
        
    def getUsdRate(self, date):
        uri = self.api_uri 
        uri = uri.replace('date', date)
        api_response = requests.get(uri)
        if api_response.status_code == 200:
            return api_response.json()["response"]["currency"]["rate_per_usd"]
        return False
    
    @property
    def rates(self):
        dates = set(str(entry[self.__datefield]) for entry in self.__data)
        for date in dates:
            if not date in self.__rates:       
                rate = self.getUsdRate(date)
                self.__rates = { date:float(rate)
                                 for date in dates if rate}
        return self.__rates        