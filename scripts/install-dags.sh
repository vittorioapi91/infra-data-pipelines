#!/bin/bash
#
# Install Airflow DAGs from src/_airflow_dags_/ to ../infra-platform/airflow/dags/
#
# This script copies DAG files from the project's airflow dags directory
# to the infra-platform repository's Airflow DAGs directory where they will be loaded by Airflow.
#
# Usage (from repo root):
#   ./scripts/install-dags.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# Source and destination directories
SOURCE_DIR="${PROJECT_ROOT}/src/_airflow_dags_"
DEST_DIR="${PROJECT_ROOT}/../infra-platform/airflow/dags"

# Check if source directory exists
if [ ! -d "${SOURCE_DIR}" ]; then
    log_warn "Source directory does not exist: ${SOURCE_DIR}"
    log_info "Creating empty directory..."
    mkdir -p "${SOURCE_DIR}"
    exit 0
fi

# Check if source directory is empty
if [ -z "$(ls -A ${SOURCE_DIR} 2>/dev/null)" ]; then
    log_warn "Source directory is empty: ${SOURCE_DIR}"
    log_info "No DAGs to install"
    exit 0
fi

# Create destination directory if it doesn't exist
mkdir -p "${DEST_DIR}"

log_info "Installing Airflow DAGs..."
log_info "  From: ${SOURCE_DIR}"
log_info "  To:   ${DEST_DIR}"

# Copy all Python files and related files from airflow-dags to ../infra-platform/airflow/dags
# Preserve directory structure
DAG_FILES=$(find "${SOURCE_DIR}" -type f \( -name "*.py" -o -name "*.md" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" \) 2>/dev/null)

if [ -z "${DAG_FILES}" ]; then
    log_warn "No DAG files found in ${SOURCE_DIR}"
    exit 0
fi

# Count files to copy
FILE_COUNT=$(echo "${DAG_FILES}" | wc -l | tr -d ' ')
log_info "Found ${FILE_COUNT} file(s) to install"

# Copy files, preserving directory structure
COPIED_COUNT=0
while IFS= read -r source_file; do
    if [ -n "${source_file}" ]; then
        # Get relative path from source directory
        rel_path="${source_file#${SOURCE_DIR}/}"
        
        # Create destination path
        dest_file="${DEST_DIR}/${rel_path}"
        dest_dir=$(dirname "${dest_file}")
        
        # Skip __init__.py if it already exists in destination (preserve original)
        if [ "${rel_path}" = "__init__.py" ] && [ -f "${dest_file}" ]; then
            log_debug "  Skipped: ${rel_path} (preserving existing file)"
            continue
        fi
        
        # Create destination directory if needed
        mkdir -p "${dest_dir}"
        
        # Copy file
        cp "${source_file}" "${dest_file}"
        COPIED_COUNT=$((COPIED_COUNT + 1))
        
        log_debug "  Copied: ${rel_path}"
    fi
done <<< "${DAG_FILES}"

log_info "✓ Installed ${COPIED_COUNT} file(s) to ${DEST_DIR}"

# List installed DAGs
PYTHON_DAGS=$(find "${DEST_DIR}" -maxdepth 1 -name "*.py" -type f 2>/dev/null | wc -l | tr -d ' ')
if [ "${PYTHON_DAGS}" -gt 0 ]; then
    log_info "Python DAG files in destination:"
    find "${DEST_DIR}" -maxdepth 1 -name "*.py" -type f -exec basename {} \; | sed 's/^/  - /'
fi
