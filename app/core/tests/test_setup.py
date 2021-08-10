import pandas as pd
from django.test import TestCase


class TestSetUp(TestCase):
    """Class with setup and teardown for tests in XIS"""

    def setUp(self):
        """Function to set up necessary data for testing"""

        # globally accessible data sets

        self.source_metadata = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "test name",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "LearningResourceIdentifier": "TestData 123",
            "SOURCESYSTEM": "JKO",
            "test_description": "test description",
            "supplemental_data": "sample1"
        }

        self.key_value = "TestData 123_JKO"
        self.key_value_hash = "0a453b6bea6e7b1d25fb9799ef734f57"
        self.hash_value = "f454114ba41034e14df2a8f3c14a047d"

        self.test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}
        self.metadata_df = pd.DataFrame.from_dict({1: self.source_metadata},
                                                  orient='index')

        return super().setUp()

    def tearDown(self):
        return super().tearDown()
