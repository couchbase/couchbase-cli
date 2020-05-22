#!/bin/bash -e

# Copyright 2020 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file  except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the  License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# FILTER allows us to pre-process the source and discard irrelevant parts.
FILTER="sed"
# Ignore source code blocks (contain arbitrary non-English configuration).
FILTER="${FILTER} -e /^----$/,/^----$/d"
# Ignore asciidoc ifdef blocks (contain arbitrary non-English configuration).
FILTER="${FILTER} -e /^ifdef/,/^endif/d"
# Ignore inline literals (contain arbitrary non-English configuration).
FILTER="${FILTER} -e s/\`[^\`]*\`//g"
# Ignore definition lists (contain arbitrary non-English configuration).
FILTER="${FILTER} -e s/^.*::$//g"
# Ignore cross-reference file names (not user-visible, however this may affect SEO).
FILTER="${FILTER} -e s/xref:[^\[]*//g"
# Ignore image file names (not user-visible, however this may affect SEO).
FILTER="${FILTER} -e s/image:[^\[]*//g"
# Ignore include statements.
FILTER="${FILTER} -e s/^include::.*\[]$//g"
# Ignore ifndef statements.
FILTER="${FILTER} -e s/^ifndef::.*\[//g"
# In the CLI we use <VAL> for meta var which sometimes are not real words so lets ignore them.
FILTER="${FILTER} -e s/<.*>//g"
# Ignore page aliases directives
FILTER="${FILTER} -e s/:page-aliases:\s\S*//g" 

# CHECK_ARGS is the spell checking command to run.
CHECK_ARGS="-l en_US --home-dir=. --add-extra-dicts=${DIR}/.aspell.en.pws"

# For each asciidoc we find in the documentation, filter out the
# stuff that isn't real text and spell check the remainder.
FAIL=""
for DOCUMENT in $(find docs/modules/cli/pages/ -type f -name '*.adoc'); do
  # Skip old adocs
  if [[ $DOCUMENT == *"/old/"* ]]; then
      continue
  fi

  MISTAKES=$(cat ${DOCUMENT} | ${FILTER} | aspell list ${CHECK_ARGS} | sort | uniq)
  if [[ ${MISTAKES} != "" ]]; then
    echo "Spell check for file ${DOCUMENT} failed:"
    echo "${MISTAKES}" | sed 's/^/    /g'
    FAIL="yes"
  fi
done

# Any failures need to be fixed up, and we need to report and error for CI.
if [[ ${FAIL} != "" ]]; then
  echo "Documentation contains errors"
  echo "    run 'aspell check ${CHECK_ARGS}' to fix typos or add to .aspell.en.pws"
  exit 1
fi
