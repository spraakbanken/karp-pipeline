#!/usr/bin/env bash

# this is a test used for Språkbankens Mink setup
# it finds all errors that has occurred when using karp-pipeline in Mink
#
# every time it is run, it saves the current date in a file, so it is easier
# to see all errors since last time the script was ran.

set -Eeuo pipefail

script_dir=$(dirname -- "${BASH_SOURCE[0]}")
script_name=$(basename -- "${BASH_SOURCE[0]}")

last_date_file="$script_dir/.last_date"

if [[ -f "$last_date_file" ]]; then
  last_date="$(cat "$last_date_file")"
else
  last_date=""
fi

# if $1 is set it overrides last_date
# use format YYYY-MM-DD
# todays date is used as a fallback
date="${1:-${last_date:-$(date +%F)}}"

echo "$date"

ssh sb02 'bash -s' < "$script_dir/_${script_name}" $date 

echo $(date +%F) > $script_dir/.last_date

