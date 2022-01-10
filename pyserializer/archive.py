# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import zipfile


def names(
    src=None,
    compression=None,
    fs=None
):
    if compression == "zip":
        data = None
        with zipfile.ZipFile(src, 'r') as zf:
            data = [{"name": name} for name in zf.namelist()]
        return data
    else:
        raise Exception("invalid archive format "+format)
    return None
