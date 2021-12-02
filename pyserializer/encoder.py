# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import datetime
import decimal
import json

import pandas as pd


class Encoder(json.JSONEncoder):

    def __init__(self, **kwargs):
        formats = kwargs.pop("formats", {})
        self.decimal_format = formats.pop("decimal", "float")
        self.date_format = formats.pop("date", "%Y-%m-%d")
        self.datetime_format = formats.pop("datetime", "%Y-%m-%dT%H:%M:%S.%f%z")
        self.timestamp_format = formats.pop("timestamp", "%Y-%m-%dT%H:%M:%S.%f%z")
        return super(Encoder, self).__init__(**kwargs)

    def default(self, obj):

        if isinstance(obj, decimal.Decimal):
            return str(obj) if self.decimal_format == "string" else float(obj)

        # timestamp comes before datetime, because a timestamp object is a subclass of datetime
        if isinstance(obj, pd.Timestamp):
            return obj.strftime(self.timestamp_format)

        # datetime comes before date, because a datetime object is a subclass of date
        if isinstance(obj, datetime.datetime):
            return obj.strftime(self.datetime_format)

        if isinstance(obj, datetime.date):
            return obj.strftime(self.date_format)

        if isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')

        return super(Encoder, self).default(obj)
