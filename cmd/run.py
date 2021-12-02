# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import fire

from pyserializer import cli


def main():
    fire.Fire(cli.CLI)


if __name__ == "__main__":
    main()
