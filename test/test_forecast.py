import unittest
from core.lib.utils.extended_collections import DotDict

from core.model.Forecast import Forecast
from datetime import datetime

class TestForecast(unittest.TestCase):

    @staticmethod
    def forecast_builder(forecast_date, campaign_first_month):
        return Forecast({
            'name': '',
            'configuration': {
                'campaign_first_month': campaign_first_month,
                'paths': []
            },
            'forecast_date': forecast_date,
            'results': {'cycle': ['NA']}
        }, {})

    def test_campaign_dates(self):
        f1 = TestForecast.forecast_builder('2015-02-01', campaign_first_month=5)
        self.assertEqual(f1.campaign_end_date, datetime.strptime('2015-04-30', '%Y-%m-%d'))
        self.assertEqual(f1.campaign_start_date, datetime.strptime('2014-05-01', '%Y-%m-%d'))
        self.assertEqual(f1.campaign_name, '2014/2015')

        f2 = TestForecast.forecast_builder('2015-02-01', campaign_first_month=9)
        self.assertEqual(f2.campaign_start_date, datetime.strptime('2014-09-01', '%Y-%m-%d'))
        self.assertEqual(f2.campaign_end_date, datetime.strptime('2015-08-31', '%Y-%m-%d'))
        self.assertEqual(f2.campaign_name, '2014/2015')

        f3 = TestForecast.forecast_builder('2015-08-31', campaign_first_month=9)
        self.assertEqual(f2.campaign_start_date, f3.campaign_start_date)
        self.assertEqual(f3.campaign_name, '2014/2015')

        f4 = TestForecast.forecast_builder('2015-08-31', campaign_first_month=5)
        self.assertEqual(f4.campaign_name, '2015/2016')
