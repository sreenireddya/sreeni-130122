import unittest
from main.BmiValues import *


class TestDataBmiValues(unittest.TestCase):

    def test_bmi_check(self):
        bmi = 29.9
        bmi_category, risk_cat = BmiValues.bmi_check(self, bmi_value=bmi)
        self.assertEqual('Overweight', bmi_category)

    def test_count_check(self):
        df_test = pd.DataFrame(
            [{"Gender": "Male", "HeightCm": 171, "WeightKg": 96, "BMI": 31.9, 'bmi_cat': 'Moderately obese'}])
        result = BmiValues.count_values(self, data = df_test)
        self.assertEqual(0, result)


if __name__ == '__main__':
    unittest.main()
