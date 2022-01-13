import pandas as pd


class BmiValues:
    # we can pass these arguments from any flow for example from airflow as a arguments
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

    # To read the json file
    def read_file(self):
        df_input = pd.read_json(self.input_path)
        return df_input

    # to write any data in CSV format we can call and use this function
    def write_file(self, dataframe):
        dataframe.to_csv(self.output_path)

    # Function to check bmi cat and risk cat based on BMI value
    def bmi_check(self, bmi_value):

        if bmi_value <= 18.4:
            bmi_cat = 'Underweight'
            health_risk = 'Malnutrition risk'

        elif bmi_value > 18.4 and bmi_value <= 24.9:
            # it will be faster than 18.4 < bmi_value <=24.9
            bmi_cat = 'Normal weight'
            health_risk = 'Low risk'
        elif bmi_value > 24.9 and bmi_value <= 29.9:
            bmi_cat = 'Overweight'
            health_risk = 'Enhanced risk'
        elif bmi_value > 30 and bmi_value <= 34.9:
            bmi_cat = 'Moderately obese'
            health_risk = 'Medium risk'
        elif bmi_value > 35 and bmi_value <= 39.9:
            bmi_cat = 'Severely obese'
            health_risk = 'High risk'
        else:
            bmi_cat = 'Very severely obese'
            health_risk = 'Very High risk'

        return bmi_cat, health_risk

    # Find BMI and add 3 columns to the data

    def transform_data(self, data):
        data["HeightCm"] = data["HeightCm"] / 100
        data['BMI'] = data["WeightKg"] / (data["HeightCm"] * data["HeightCm"])
        data['BMI'] = data['BMI'].round(decimals=1)
        data['bmi_cat'], data['health_risk'] = zip(*data['BMI'].apply(self.bmi_check))

        return data

    # Function will be used to count the number of rows with BMI cat as overweight
    def count_values(self, data):
        count_data = data.groupby('bmi_cat').size().reset_index(name='counts')
        count_data = count_data.loc[count_data['bmi_cat'] == 'over weight']
        # if there are no overweight persons it will raise error as the data frame is empty
        try:
            result = count_data['counts'].values[0]
        except:
            result = 0
        return result
    # if you need a cumulative count for overwrite we can filter the data frame on BMI where
    # BMI >=25 and count number of rows will be faster to do that ,
    # if you are reading data from a saved file sort and filter will be faster in that scenario
