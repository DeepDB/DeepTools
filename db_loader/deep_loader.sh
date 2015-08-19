#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# High Performance Parallel 'mysqldump' loader

#
# Command line options:
#
# python deep_loader.py --help

#
# Command line example: script arguments are "db_name" and "dumpfile"
#
python deep_loader.py --debug --drop --threads=10 --db_engine=Deep --db_user=root --db_password=foobar --db_name=$1 --dumpfile=$2
