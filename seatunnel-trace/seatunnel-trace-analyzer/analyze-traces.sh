#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# SeaTunnel StainTrace Analyzer - Shell Wrapper
# Usage: ./analyze-traces.sh [input_dir] [output_html] [job_id] [date]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
INPUT_DIR="${1:-/tmp/seatunnel/traces}"
OUTPUT_HTML="${2:-trace-report.html}"
JOB_ID="$3"
DATE="$4"

# Check if JAR exists
JAR_FILE="$SCRIPT_DIR/target/seatunnel-trace-analyzer-*-jar-with-dependencies.jar"
if ! ls $JAR_FILE 1> /dev/null 2>&1; then
    echo "Error: JAR file not found at $JAR_FILE"
    echo "Please run 'mvn clean package' first"
    exit 1
fi

# Get the actual JAR file path
JAR_PATH=$(ls $JAR_FILE | head -1)

# Build command
CMD="java -jar \"$JAR_PATH\" -i \"$INPUT_DIR\" -o \"$OUTPUT_HTML\""

if [ -n "$JOB_ID" ]; then
    CMD="$CMD -j \"$JOB_ID\""
fi

if [ -n "$DATE" ]; then
    CMD="$CMD -d \"$DATE\""
fi

echo "SeaTunnel StainTrace Analyzer"
echo "=============================="
echo "Input directory: $INPUT_DIR"
echo "Output file: $OUTPUT_HTML"
[ -n "$JOB_ID" ] && echo "Job ID filter: $JOB_ID"
[ -n "$DATE" ] && echo "Date filter: $DATE"
echo ""

# Execute
eval $CMD

echo ""
echo "Done! Open the report:"
echo "  open $OUTPUT_HTML  # macOS"
echo "  xdg-open $OUTPUT_HTML  # Linux"
