# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import pandas._libs.missing as libmissing


def clean(obj, drop_nulls=True, drop_blanks=True):
    if isinstance(obj, dict):
        return {
            k: clean(v, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
            for k, v in obj.items() if ((not libmissing.checknull(v)) or not drop_nulls) and (v != "" or not drop_blanks) # noqa
        }
    elif isinstance(obj, list):
        return [
            clean(x, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
            for x in obj
        ]
    return obj
