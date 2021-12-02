#!/bin/bash

# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

testdata_local="${DIR}/../testdata"

_testRoundtrip() {
  local src=${1}
  local dest=${2}
  local format=${3}
  local compression=${4:-}
  echo "${src} => ${dest} (${compression})"
  echo "there"
  python3 cmd/run.py transform \
  --src=${src} \
  --dest="${dest}/there" \
  --input-format=jsonl \
  --output-compression="${compression}" \
  --output-format="${format}"
  echo "back"
  python3 cmd/run.py transform \
  --src="${dest}/there" \
  --dest="${dest}/back" \
  --input-compression="${compression}" \
  --input-format="${format}" \
  --output-format=jsonl \
  --drop-blanks \
  --drop-nulls
  local expected=$(cat ${src})
  local output=$(cat "${dest}/back")
  assertEquals "unexpected output" "${expected}" "${output}"
}

testRoundtripCSV() {
  mkdir -p "${SHUNIT_TMPDIR}/testRoundtripCSV"
  _testRoundtrip "${testdata_local}/doc.jsonl" "${SHUNIT_TMPDIR}/testRoundtripCSV" "csv"
}

testRoundtripCSVGZIP() {
  mkdir -p "${SHUNIT_TMPDIR}/testRoundtripCSVGZIP"
  _testRoundtrip "${testdata_local}/doc.jsonl" "${SHUNIT_TMPDIR}/testRoundtripCSVGZIP" "csv" "gzip"
}

testRoundtripJSON() {
  mkdir -p "${SHUNIT_TMPDIR}/testRoundtripJSON"
  _testRoundtrip "${testdata_local}/doc.jsonl" "${SHUNIT_TMPDIR}/testRoundtripJSON" "json"
}

testRoundtripJSONGZIP() {
  mkdir -p "${SHUNIT_TMPDIR}/testRoundtripJSONGZIP"
  _testRoundtrip "${testdata_local}/doc.jsonl" "${SHUNIT_TMPDIR}/testRoundtripJSONGZIP" "json" "gzip"
}

testRoundtripParquet() {
  mkdir -p "${SHUNIT_TMPDIR}/testRoundtripParquet"
  _testRoundtrip "${testdata_local}/doc.jsonl" "${SHUNIT_TMPDIR}/testRoundtripParquet" "parquet"
}

testAlgorithms() {
  python3 cmd/run.py algorithms
}

testFormats() {
  python3 cmd/run.py formats
}

oneTimeSetUp() {
  echo "Using temporary directory at ${SHUNIT_TMPDIR}"
  echo "Reading testdata from ${testdata_local}"
}

oneTimeTearDown() {
  echo "Tearing Down"
}

# Load shUnit2.
. "${DIR}/shunit2"
