from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class DateHelper:
    """DateHelper is a class to support date transformation and date list generation.
    """
    def __init__(self, input_type: str="string", input_format: str="%Y-%m-%d", output_type: str="string", output_format: str="%Y-%m-%d"):
        """Constructs all the necessary attributes for the DateHelper

        Attributes:
            input_type (str): input date type.
            input_format (str): input date format for the methods if input_type is string.
            output_type (str): output date type.
            output_format (str): output date format for the methods if output_type is string.
        """
        self.input_type = input_type
        self.input_format = input_format
        self.output_type = output_type
        self.output_format = output_format
    # ------------------------------
    # property
    @property
    def input_type(self):
        return self._input_type

    @input_type.setter
    def input_type(self, value):
        if value not in {"string", "datetime"}:
            raise ValueError("Unknown input_type param value. Only accepts (string, datetime)")
        self._input_type = value

    @property
    def output_type(self):
        return self._output_type

    @output_type.setter
    def output_type(self, value):
        if value not in {"string", "datetime"}:
            raise ValueError("Unknown output_type param value. Only accepts (string, datetime)")
        self._output_type = value
    # ------------------------------
    # general
    def transform_format(self, input_date, from_type: str, to_type: str):
        """Transform the date from input format to output format

        Args:
            input_date: the date that need to be transformed
            from_type: the data type of the input_date
            to_type: the ouput data type of the input_date

        Returns:
            
        """
        if (from_type == "string") & (to_type == "datetime"):
            return datetime.strptime(input_date, self.input_format)
        elif (from_type == "datetime") & (to_type == "string"):
            return input_date.strftime(self.output_format)
        else:
            return input_date
    # ------------------------------
    # date related method
    def get_first_last_date_of_month(self, input_date) -> tuple:
        """
        """
        input_date = self.transform_format(input_date, self.input_type, "datetime")
        first_date = self.transform_format(input_date.replace(day=1), "datetime", self.output_type)
        end_date = self.transform_format(input_date.replace(day=1)+relativedelta(months=1)-timedelta(days=1), "datetime", self.output_type)
        return (first_date, end_date)

    def generate_date_list(self, start_date, end_date) -> list:
        """
        """
        day_diff = self.calculate_day_difference(start_date, end_date)
        start_date = self.transform_format(start_date, self.input_type, "datetime")
        end_date = self.transform_format(end_date, self.input_type, "datetime")
        date_list =  sorted([
            self.transform_format(start_date+timedelta(days=i), "datetime", self.output_type) for i in range(day_diff+1)
        ])
        return date_list

    def calculate_day_difference(self, start_date, end_date) -> int:
        """
        """
        start_date = self.transform_format(start_date, self.input_type, "datetime")
        end_date = self.transform_format(end_date, self.input_type, "datetime")
        diff = (end_date-start_date).days
        return diff
    # ------------------------------
    # month related method
    def generate_month_list(self, start_date, end_date) -> list:
        """
        """
        month_diff = self.calculate_month_difference(start_date, end_date)
        start_date = self.transform_format(start_date, self.input_type, "datetime")
        end_date = self.transform_format(end_date, self.input_type, "datetime")
        date_list = sorted([
            self.transform_format(start_date+relativedelta(months=i), "datetime", self.output_type) for i in range(month_diff+1)
        ])
        return date_list

    def calculate_month_difference(self, start_date, end_date) -> int:
        """
        """
        start_date = self.transform_format(start_date, self.input_type, "datetime")
        end_date = self.transform_format(end_date, self.input_type, "datetime")
        diff = relativedelta(end_date, start_date)
        diff = diff.years * 12 + diff.months
        return diff