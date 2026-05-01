#!/bin/bash
#
# Build environment-specific wheels for TradingPythonAgent
#
# Usage (from repo root):
#   ./scripts/build-wheel.sh [dev|staging|prod] [--linux-x86_64] [--win-x86_64] [--macosx-arm64]
#
# If no environment is specified, automatically detects from git branch:
#   - dev/* branches → dev
#   - staging branch → staging
#   - main/master branch → prod
#
# Platform flags (at least one must be specified):
#   --linux-x86_64   Build Linux 64-bit (linux_x86_64) wheel
#   --linux-arm64    Build Linux ARM64 (linux_arm64) wheel
#   --win-x86_64     Build Windows 64-bit (win_amd64) wheel
#   --macosx-arm64   Build macOS ARM64 (macosx_arm64) wheel
#
# At least one platform flag must be provided when executing the script.
#
# Wheels will be named: trading_agent-{version}-{platform}.whl
# Wheels are organized in environment-specific folders: dist/{env}/
# All .egg-info directories are moved to dist/{env}/ directory (kept with wheels)
# build/ directory is completely removed after builds (not needed)
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

# Function to get current git branch
get_git_branch() {
    local branch
    if command -v git &> /dev/null; then
        branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
        if [ -n "$branch" ]; then
            echo "$branch"
            return 0
        fi
    fi
    # Fallback to environment variable
    echo "${GIT_BRANCH:-${BRANCH_NAME:-}}"
}

# Function to convert to lowercase (bash 3 compatible)
to_lower() {
    echo "$1" | tr '[:upper:]' '[:lower:]'
}

# Function to determine environment from branch
get_env_from_branch() {
    local branch="$1"
    local branch_lower=$(to_lower "$branch")
    
    # Check for staging branch
    if [[ "$branch_lower" == "staging" ]]; then
        echo "staging"
        return 0
    fi
    
    # Check for main/master branch
    if [[ "$branch_lower" == "main" || "$branch_lower" == "master" ]]; then
        echo "prod"
        return 0
    fi
    
    # Check for dev branches (dev/* or starts with dev)
    if [[ "$branch_lower" =~ ^dev/ ]] || [[ "$branch_lower" =~ ^dev ]]; then
        echo "dev"
        return 0
    fi
    
    # Default to dev for any other branch
    echo "dev"
}

# Parse command-line arguments
EXPLICIT_ENV=""
BUILD_LINUX_X86_64=false
BUILD_LINUX_ARM64=false
BUILD_WIN_X86_64=false
BUILD_MACOSX_ARM64=false

while [[ $# -gt 0 ]]; do
    case $1 in
        dev|staging|prod)
            EXPLICIT_ENV=$(to_lower "$1")
            shift
            ;;
        --linux-x86_64)
            BUILD_LINUX_X86_64=true
            shift
            ;;
        --linux-arm64)
            BUILD_LINUX_ARM64=true
            shift
            ;;
        --win-x86_64)
            BUILD_WIN_X86_64=true
            shift
            ;;
        --macosx-arm64)
            BUILD_MACOSX_ARM64=true
            shift
            ;;
        *)
            log_warn "Unknown argument: $1"
            shift
            ;;
    esac
done

# Validate environment if explicitly provided
if [ -n "$EXPLICIT_ENV" ] && [[ ! "$EXPLICIT_ENV" =~ ^(dev|staging|prod)$ ]]; then
    log_warn "Invalid environment: $EXPLICIT_ENV. Auto-detecting from branch..."
    EXPLICIT_ENV=""
fi

# Validate that at least one platform flag is provided
if [ "$BUILD_LINUX_X86_64" = false ] && [ "$BUILD_LINUX_ARM64" = false ] && [ "$BUILD_WIN_X86_64" = false ] && [ "$BUILD_MACOSX_ARM64" = false ]; then
    echo "Error: At least one platform flag must be provided."
    echo "Usage: $0 [dev|staging|prod] [--linux-x86_64] [--linux-arm64] [--win-x86_64] [--macosx-arm64]"
    echo ""
    echo "Platform flags:"
    echo "  --linux-x86_64   Build Linux 64-bit (linux_x86_64) wheel"
    echo "  --linux-arm64    Build Linux ARM64 (linux_arm64) wheel"
    echo "  --win-x86_64     Build Windows 64-bit (win_amd64) wheel"
    echo "  --macosx-arm64   Build macOS ARM64 (macosx_arm64) wheel"
    exit 1
fi

# Track if we need to restore the original branch
ORIGINAL_BRANCH=""
RESTORE_BRANCH=false

# Check if we're in a CI environment (Jenkins, GitHub Actions, etc.)
# In CI, source is already checked out, so skip checkout
IS_CI=false
if [ -n "${CI:-}" ] || [ -n "${JENKINS_HOME:-}" ] || [ -n "${BUILD_NUMBER:-}" ]; then
    IS_CI=true
    log_info "CI environment detected. Skipping branch checkout (source already checked out)."
fi

# If environment is explicitly provided, checkout the corresponding branch from origin
# (unless we're in CI, where source is already checked out)
if [ -n "$EXPLICIT_ENV" ] && [ "$IS_CI" = false ]; then
    ORIGINAL_BRANCH=$(get_git_branch)
    if [ -n "$ORIGINAL_BRANCH" ]; then
        RESTORE_BRANCH=true
    fi
    
    log_info "Environment explicitly specified: $EXPLICIT_ENV"
    log_info "Checking out corresponding branch from origin..."
    
    case "$EXPLICIT_ENV" in
        dev)
            # For dev, checkout the latest dev/* branch from origin
            # First, fetch all branches
            git fetch origin --prune 2>/dev/null || log_warn "Could not fetch from origin"
            
            # Find the latest dev/* branch (by commit date)
            DEV_BRANCH=$(git branch -r --sort=-committerdate 2>/dev/null | grep -E 'origin/dev/' | head -n 1 | sed 's|origin/||' | xargs)
            
            if [ -z "$DEV_BRANCH" ]; then
                log_warn "No dev/* branch found on origin. Using current branch."
                ENV="dev"
            else
                log_info "Found latest dev branch on origin: $DEV_BRANCH"
                if git checkout "$DEV_BRANCH" 2>/dev/null; then
                    git pull origin "$DEV_BRANCH" 2>/dev/null || true
                    ENV="dev"
                elif git checkout -b "$DEV_BRANCH" "origin/$DEV_BRANCH" 2>/dev/null; then
                    ENV="dev"
                else
                    log_warn "Could not checkout $DEV_BRANCH. Using current branch."
                    ENV="dev"
                    RESTORE_BRANCH=false
                fi
            fi
            ;;
        staging)
            log_info "Checking out 'staging' branch from origin..."
            git fetch origin staging 2>/dev/null || log_warn "Could not fetch staging from origin"
            if git checkout staging 2>/dev/null; then
                git pull origin staging 2>/dev/null || true
                ENV="staging"
            else
                log_warn "Could not checkout staging. Using current branch."
                ENV="staging"
                RESTORE_BRANCH=false
            fi
            ;;
        prod)
            # Try main first, then master
            if git ls-remote --heads origin main 2>/dev/null | grep -q main; then
                log_info "Checking out 'main' branch from origin..."
                git fetch origin main 2>/dev/null || log_warn "Could not fetch main from origin"
                if git checkout main 2>/dev/null; then
                    git pull origin main 2>/dev/null || true
                    ENV="prod"
                else
                    log_warn "Could not checkout main. Using current branch."
                    ENV="prod"
                    RESTORE_BRANCH=false
                fi
            elif git ls-remote --heads origin master 2>/dev/null | grep -q master; then
                log_info "Checking out 'master' branch from origin..."
                git fetch origin master 2>/dev/null || log_warn "Could not fetch master from origin"
                if git checkout master 2>/dev/null; then
                    git pull origin master 2>/dev/null || true
                    ENV="prod"
                else
                    log_warn "Could not checkout master. Using current branch."
                    ENV="prod"
                    RESTORE_BRANCH=false
                fi
            else
                log_warn "Neither 'main' nor 'master' branch found on origin. Using current branch."
                ENV="prod"
                RESTORE_BRANCH=false
            fi
            ;;
    esac
    
    CURRENT_BRANCH=$(get_git_branch)
    log_info "Now on branch: $CURRENT_BRANCH"
elif [ -n "$EXPLICIT_ENV" ] && [ "$IS_CI" = true ]; then
    # In CI, use the explicitly provided environment but don't checkout
    ENV="$EXPLICIT_ENV"
    CURRENT_BRANCH=$(get_git_branch)
    log_info "CI environment: Using explicitly specified environment '$ENV' on branch '$CURRENT_BRANCH'"
else
    # Auto-detect environment from current branch (don't checkout)
    CURRENT_BRANCH=$(get_git_branch)
    if [ -n "$CURRENT_BRANCH" ]; then
        ENV=$(get_env_from_branch "$CURRENT_BRANCH")
        log_info "Auto-detected environment from current branch '$CURRENT_BRANCH': $ENV"
    else
        log_warn "Could not determine git branch. Defaulting to 'dev'"
        ENV="dev"
    fi
fi

log_info "Building wheel for environment: $ENV"

# Check if setuptools and wheel are installed
if ! python3 -c "import setuptools" 2>/dev/null; then
    log_warn "setuptools not found. Installing..."
    pip install setuptools
fi

if ! python3 -c "import wheel" 2>/dev/null; then
    log_warn "wheel not found. Installing..."
    pip install wheel
fi

# Create environment-specific dist directory (wheels go here)
mkdir -p "${PROJECT_ROOT}/dist/${ENV}"

# Clean previous builds
log_info "Cleaning previous builds..."
rm -rf "${PROJECT_ROOT}/build" "${PROJECT_ROOT}/src/*.egg-info"
find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
# Remove existing wheels in dist/${ENV}/ so new build replaces them (no duplicates when version is same)
rm -f "${PROJECT_ROOT}/dist/${ENV}"/trading_agent-*.whl 2>/dev/null || true
# Also clean .egg-info from dist/{ENV}/ (they'll be regenerated)
find "${PROJECT_ROOT}/dist/${ENV}" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Build wheel with standard package name
log_info "Building wheel: trading_agent-*.whl (will be placed in dist/${ENV}/)"
cd "${PROJECT_ROOT}"

# Detect current platform
CURRENT_PLATFORM=$(python3 -c "import platform; print(platform.machine().lower())" 2>/dev/null || echo "unknown")
CURRENT_SYSTEM=$(python3 -c "import sys; print(sys.platform)" 2>/dev/null || echo "unknown")

log_info "Current platform: ${CURRENT_PLATFORM} (${CURRENT_SYSTEM})"

# Function to get macOS platform tag (Python 3.13+ format: macosx_26_0_arm64)
get_macos_platform_tag() {
    local macos_version=$(python3 -c "import platform; v=platform.mac_ver()[0]; print(v)" 2>/dev/null || echo "0.0")
    local major=$(echo "$macos_version" | cut -d. -f1)
    local minor=$(echo "$macos_version" | cut -d. -f2)
    local arch=$(python3 -c "import platform; print(platform.machine().lower())" 2>/dev/null || echo "arm64")
    echo "macosx_${major}_${minor}_${arch}"
}

# Build macOS ARM64 wheel (default)
if [ "$BUILD_MACOSX_ARM64" = true ]; then
    MACOS_PLATFORM_TAG=$(get_macos_platform_tag)
    log_info "Building macOS ARM64 wheel with platform tag: ${MACOS_PLATFORM_TAG}"
    ENV="${ENV}" python3 setup.py bdist_wheel --plat-name="${MACOS_PLATFORM_TAG}" --dist-dir="${PROJECT_ROOT}/dist/${ENV}"
    
    # Move .egg-info to dist/${ENV}/ directory after build (keep with wheels)
    # Check both project root and src/ (setuptools creates it in project root)
    if [ -n "$(find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving .egg-info directories to dist/${ENV}/..."
        find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    if [ -n "$(find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving additional .egg-info directories from src/ to dist/${ENV}/..."
        find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Move any wheels from dist/ to dist/${ENV}/ (in case they weren't built directly there)
    if [ -d "${PROJECT_ROOT}/dist" ]; then
        find "${PROJECT_ROOT}/dist" -maxdepth 1 -name "trading_agent-*.whl" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
fi

# Clean up build/ directory completely (setuptools artifacts not needed)
if [ -d "${PROJECT_ROOT}/build" ]; then
    log_info "Cleaning up build/ directory..."
    rm -rf "${PROJECT_ROOT}/build" 2>/dev/null || true
fi

# Build Linux 64-bit wheel (optional)
if [ "$BUILD_LINUX_X86_64" = true ]; then
    log_info "Building Linux 64-bit (linux_x86_64) wheel..."
    ENV="${ENV}" python3 setup.py bdist_wheel --plat-name=linux_x86_64 --dist-dir="${PROJECT_ROOT}/dist/${ENV}"
    
    # Move any additional .egg-info directories to dist/${ENV}/
    # Check both project root and src/ (setuptools creates it in project root)
    if [ -n "$(find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving .egg-info directories to dist/${ENV}/..."
        find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    if [ -n "$(find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving additional .egg-info directories from src/ to dist/${ENV}/..."
        find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Move any wheels from dist/ to dist/${ENV}/
    if [ -d "${PROJECT_ROOT}/dist" ]; then
        find "${PROJECT_ROOT}/dist" -maxdepth 1 -name "trading_agent-*.whl" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Clean up build/ directory completely after each build
    rm -rf "${PROJECT_ROOT}/build" 2>/dev/null || true
fi

# Build Linux ARM64 wheel (optional)
if [ "$BUILD_LINUX_ARM64" = true ]; then
    log_info "Building Linux ARM64 (manylinux2014_aarch64) wheel..."
    ENV="${ENV}" python3 setup.py bdist_wheel --plat-name=manylinux2014_aarch64 --dist-dir="${PROJECT_ROOT}/dist/${ENV}"
    
    # Move .egg-info to dist/${ENV}/ (keep with wheels). Check project root and src/.
    if [ -n "$(find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving .egg-info directories to dist/${ENV}/..."
        find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    if [ -n "$(find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving additional .egg-info directories to dist/${ENV}/..."
        find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Move any wheels from dist/ to dist/${ENV}/
    if [ -d "${PROJECT_ROOT}/dist" ]; then
        find "${PROJECT_ROOT}/dist" -maxdepth 1 -name "trading_agent-*.whl" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Clean up build/ directory completely after each build
    rm -rf "${PROJECT_ROOT}/build" 2>/dev/null || true
fi

# Build Windows 64-bit wheel (optional)
if [ "$BUILD_WIN_X86_64" = true ]; then
    log_info "Building Windows 64-bit (win_amd64) wheel..."
    ENV="${ENV}" python3 setup.py bdist_wheel --plat-name=win_amd64 --dist-dir="${PROJECT_ROOT}/dist/${ENV}"
    
    # Move any additional .egg-info directories to dist/${ENV}/
    # Check both project root and src/ (setuptools creates it in project root)
    if [ -n "$(find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving .egg-info directories to dist/${ENV}/..."
        find "${PROJECT_ROOT}" -maxdepth 1 -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    if [ -n "$(find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" 2>/dev/null | head -1)" ]; then
        log_info "Moving additional .egg-info directories from src/ to dist/${ENV}/..."
        find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Move any wheels from dist/ to dist/${ENV}/
    if [ -d "${PROJECT_ROOT}/dist" ]; then
        find "${PROJECT_ROOT}/dist" -maxdepth 1 -name "trading_agent-*.whl" -exec mv {} "${PROJECT_ROOT}/dist/${ENV}/" \; 2>/dev/null || true
    fi
    
    # Clean up build/ directory completely after each build
    rm -rf "${PROJECT_ROOT}/build" 2>/dev/null || true
fi

# Final cleanup: ensure build/ directory is removed (all artifacts in dist/)
if [ -d "${PROJECT_ROOT}/build" ]; then
    log_info "Removing build/ directory (all artifacts in dist/)..."
    rm -rf "${PROJECT_ROOT}/build" 2>/dev/null || true
fi

# Find all generated wheels in the environment-specific directory
# Package name is trading_agent (no env suffix)
WHEEL_FILES=$(find "${PROJECT_ROOT}/dist/${ENV}" -name "trading_agent-*.whl" 2>/dev/null | sort)

if [ -z "${WHEEL_FILES}" ]; then
    log_warn "⚠️  No wheel files found in dist/${ENV}/ directory"
    exit 1
fi

# Display all built wheels
log_info "Built wheels:"
while IFS= read -r wheel_file; do
    if [ -n "${wheel_file}" ]; then
        wheel_name=$(basename "${wheel_file}")
        wheel_size=$(du -h "${wheel_file}" | cut -f1)
        log_info "  ✓ ${wheel_name} (${wheel_size})"
    fi
done <<< "${WHEEL_FILES}"

# Use the first wheel for contents display (prefer macosx_arm64, otherwise first available)
SAMPLE_WHEEL=$(echo "${WHEEL_FILES}" | grep "macosx_arm64" | head -n 1 || true)
if [ -z "${SAMPLE_WHEEL}" ]; then
    SAMPLE_WHEEL=$(echo "${WHEEL_FILES}" | head -n 1)
fi

if [ -n "${SAMPLE_WHEEL}" ]; then
    log_info "Wheel contents (sample from $(basename "${SAMPLE_WHEEL}")):"
    unzip -l "${SAMPLE_WHEEL}" 2>/dev/null | head -n 20 || true
    # Note: head may exit with non-zero if fewer lines than requested, so || true prevents script failure
fi

log_info "Build complete!"

# Restore original branch if we checked out a different one
if [ "$RESTORE_BRANCH" = true ] && [ -n "$ORIGINAL_BRANCH" ]; then
    CURRENT_BRANCH=$(get_git_branch)
    if [ "$CURRENT_BRANCH" != "$ORIGINAL_BRANCH" ]; then
        log_info "Restoring original branch: $ORIGINAL_BRANCH"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || log_warn "Could not restore branch $ORIGINAL_BRANCH"
    fi
fi

# Explicitly exit with success code
exit 0
