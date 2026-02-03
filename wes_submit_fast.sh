#!/usr/bin/env bash
################################################################################
# WES Pipeline - PBS Array Job Submitter
# =======================================
#
# USAGE:
#   ./wes_submit.sh                    # Use default config.yaml in same directory
#   ./wes_submit.sh /path/to/config.yaml  # Use specified config file
#
# This is a THIN wrapper that:
#   1. Detects all samples from input directory
#   2. Creates a PBS array job that calls wes_pipline.sh for each sample
#   3. All actual processing logic stays in wes_pipline.sh (unchanged)
#
# The original wes_pipline.sh handles everything:
#   - Backup, cleanup, directory creation
#   - Alignment, variant calling, joint genotyping, VQSR
#   - Same output structure as before
################################################################################

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   WES PBS Array Job Submitter${NC}"
echo -e "${BLUE}========================================${NC}"
echo "Started: $(date)"
echo ""

################################################################################
# Configuration
################################################################################

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Config file - use argument or default to config_fast.yaml in script directory
CONFIG_FILE="${1:-${SCRIPT_DIR}/config_fast.yaml}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}ERROR: Configuration file not found: $CONFIG_FILE${NC}"
    echo ""
    echo "Usage: $0 [config.yaml]"
    exit 1
fi

echo -e "${GREEN}Using configuration:${NC} $CONFIG_FILE"

# Check that wes_pipline_fast.sh exists
WES_PIPELINE="${SCRIPT_DIR}/wes_pipline_fast.sh"
if [ ! -f "$WES_PIPELINE" ]; then
    echo -e "${RED}ERROR: wes_pipline_fast.sh not found in: $SCRIPT_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}Using pipeline:${NC} $WES_PIPELINE"

################################################################################
# YAML Parser (same as wes_pipline.sh)
################################################################################

parse_yaml() {
    local yaml_file=$1
    local prefix=${2:-}
    local s='[[:space:]]*'
    local w='[a-zA-Z0-9_]*'
    local fs=$(echo @|tr @ '\034')

    sed -ne "s|^\($s\):|\1|" \
         -e "s|^\($s\)\($w\)$s:$s[\"\']\\(.*\\)[\"\']$s\$|\1$fs\2$fs\3|p" \
         -e "s|^\($s\)\($w\)$s:$s\\(.*\\)$s\$|\1$fs\2$fs\3|p" "$yaml_file" |
    awk -F"$fs" '{
        indent = length($1)/2;
        vname[indent] = $2;
        for (i in vname) {if (i > indent) {delete vname[i]}}
        if (length($3) > 0) {
            vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
            printf("%s%s%s=\"%s\"\n", "'"$prefix"'",vn, $2, $3);
        }
    }'
}

################################################################################
# Load Configuration
################################################################################

echo ""
echo -e "${YELLOW}Loading configuration...${NC}"

eval $(parse_yaml "$CONFIG_FILE" "CONFIG_")

# Core directories
INPUT_DIR="${CONFIG_directories_input}"
OUTPUT_DIR="${CONFIG_directories_output}"
TEMP_DIR="${CONFIG_directories_temp}"

# Processing parameters
THREADS="${CONFIG_processing_threads:-16}"
MEMORY_GB="${CONFIG_processing_memory_gb:-32}"

# PBS settings
PBS_QUEUE="${CONFIG_pbs_queue:-cpu}"
PBS_WALLTIME="${CONFIG_pbs_walltime:-24:00:00}"

# Project name
PROJECT_NAME="${CONFIG_project_name:-wes_project}"

echo "  Project: $PROJECT_NAME"
echo "  Input Directory: $INPUT_DIR"
echo "  Output Directory: $OUTPUT_DIR"
echo "  PBS Queue: $PBS_QUEUE"

################################################################################
# Detect Input FASTQ Files
################################################################################

echo ""
echo -e "${YELLOW}Detecting input FASTQ files...${NC}"

if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${RED}ERROR: Input directory not found: $INPUT_DIR${NC}"
    exit 1
fi

cd "$INPUT_DIR"

# Try multiple common FASTQ naming patterns
FASTQ_SUFFIX=""
R1_FILES=()

# Pattern 1: *_1.clean.fastq.gz
mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.clean.fastq.gz" -type f 2>/dev/null | sort)
if [ ${#R1_FILES[@]} -gt 0 ]; then
    FASTQ_SUFFIX="_1.clean.fastq.gz"
fi

# Pattern 2: *_1.trimmed.fastq.gz
if [ ${#R1_FILES[@]} -eq 0 ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.trimmed.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_SUFFIX="_1.trimmed.fastq.gz"
    fi
fi

# Pattern 3: *_1.fastq.gz
if [ ${#R1_FILES[@]} -eq 0 ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_SUFFIX="_1.fastq.gz"
    fi
fi

# Pattern 4: *_R1*.fastq.gz
if [ ${#R1_FILES[@]} -eq 0 ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_R1*.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_SUFFIX="_R1.fastq.gz"
    fi
fi

if [ ${#R1_FILES[@]} -eq 0 ]; then
    echo -e "${RED}ERROR: No FASTQ files found in $INPUT_DIR${NC}"
    echo "Supported patterns: *_1.clean.fastq.gz, *_1.trimmed.fastq.gz, *_1.fastq.gz, *_R1*.fastq.gz"
    exit 1
fi

# Extract sample names
SAMPLES=()
for R1 in "${R1_FILES[@]}"; do
    SAMPLE=$(basename "$R1" "$FASTQ_SUFFIX")
    SAMPLES+=("$SAMPLE")
done

TOTAL_SAMPLES=${#SAMPLES[@]}
echo -e "${GREEN}Found $TOTAL_SAMPLES samples${NC}"
echo "FASTQ pattern: $FASTQ_SUFFIX"
echo ""
echo "Samples:"
for i in "${!SAMPLES[@]}"; do
    echo "  [$i] ${SAMPLES[$i]}"
done

cd "$SCRIPT_DIR"

################################################################################
# Backup Previous Run (if any) - Same logic as wes_pipline.sh
################################################################################

echo ""
echo -e "${YELLOW}Checking for existing data to backup...${NC}"

# Backup directory location (from config or default)
BACKUP_BASE_DIR="${CONFIG_directories_backup:-$(dirname "$OUTPUT_DIR")/backup}"
RUN_DATE=$(date '+%Y_%m_%d_%a')
BACKUP_DIR="$BACKUP_BASE_DIR/backup_${RUN_DATE}"

# Check if there's anything to backup
BACKUP_NEEDED=false

if [ -d "$OUTPUT_DIR" ]; then
    CRAM_COUNT=$(find "$OUTPUT_DIR" -name "*.cram" 2>/dev/null | wc -l)
    VCF_COUNT=$(find "$OUTPUT_DIR" -name "*.vcf.gz" 2>/dev/null | wc -l)
    LOG_COUNT=$(find "$OUTPUT_DIR" -name "*.log" 2>/dev/null | wc -l)
    QC_COUNT=$(find "$OUTPUT_DIR" -path "*/qc_stats*" -name "*.txt" 2>/dev/null | wc -l)

    if [ "$CRAM_COUNT" -gt 0 ] || [ "$VCF_COUNT" -gt 0 ] || [ "$LOG_COUNT" -gt 0 ] || [ "$QC_COUNT" -gt 0 ]; then
        BACKUP_NEEDED=true
    fi
fi

if [ "$BACKUP_NEEDED" = "true" ]; then
    echo -e "${GREEN}Found existing data to backup:${NC}"
    echo "  CRAM files: $CRAM_COUNT"
    echo "  VCF files: $VCF_COUNT"
    echo "  Log files: $LOG_COUNT"
    echo "  QC stat files: $QC_COUNT"
    echo ""
    echo "Creating backup at: $BACKUP_DIR"

    mkdir -p "$BACKUP_DIR/cram"
    mkdir -p "$BACKUP_DIR/vcf"
    mkdir -p "$BACKUP_DIR/logs"
    mkdir -p "$BACKUP_DIR/qc_stats"

    # Backup with sample name folders
    if [ "$CRAM_COUNT" -gt 0 ]; then
        echo "  Backing up CRAM files..."
        for cram_file in $(find "$OUTPUT_DIR" -name "*.cram" 2>/dev/null); do
            sample_name=$(basename "$cram_file" .dedup.cram)
            mkdir -p "$BACKUP_DIR/cram/$sample_name"
            cp "$cram_file" "$BACKUP_DIR/cram/$sample_name/" 2>/dev/null || true
            [ -f "${cram_file}.crai" ] && cp "${cram_file}.crai" "$BACKUP_DIR/cram/$sample_name/" 2>/dev/null || true
        done
        # Copy reference
        for cram_dir in "$OUTPUT_DIR"/cram_*; do
            if [ -f "$cram_dir/reference.fa" ]; then
                cp -L "$cram_dir/reference.fa" "$BACKUP_DIR/cram/" 2>/dev/null || true
                cp -L "$cram_dir/reference.fa.fai" "$BACKUP_DIR/cram/" 2>/dev/null || true
                break
            fi
        done
    fi

    if [ "$VCF_COUNT" -gt 0 ]; then
        echo "  Backing up VCF files..."
        find "$OUTPUT_DIR" -name "*.vcf.gz" -exec cp {} "$BACKUP_DIR/vcf/" \; 2>/dev/null || true
        find "$OUTPUT_DIR" -name "*.vcf.gz.tbi" -exec cp {} "$BACKUP_DIR/vcf/" \; 2>/dev/null || true
    fi

    if [ "$LOG_COUNT" -gt 0 ]; then
        echo "  Backing up log files..."
        for log_dir in "$OUTPUT_DIR"/logs_*/; do
            [ -d "$log_dir" ] && cp -r "$log_dir" "$BACKUP_DIR/logs/" 2>/dev/null || true
        done
    fi

    if [ "$QC_COUNT" -gt 0 ]; then
        echo "  Backing up QC statistics..."
        for qc_dir in "$OUTPUT_DIR"/qc_stats_*/; do
            [ -d "$qc_dir" ] && cp -r "$qc_dir" "$BACKUP_DIR/qc_stats/" 2>/dev/null || true
        done
    fi

    echo -e "${GREEN}Backup complete: $BACKUP_DIR${NC}"
else
    echo "No existing data to backup."
fi

################################################################################
# Clean Previous Run (if any)
################################################################################

echo ""
echo -e "${YELLOW}Cleaning previous run data...${NC}"

if [ -d "$TEMP_DIR" ]; then
    echo "  Removing temp directory: $TEMP_DIR"
    rm -rf "$TEMP_DIR"
fi

if [ -d "$OUTPUT_DIR" ]; then
    echo "  Removing output directory: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi

echo -e "${GREEN}Cleanup complete!${NC}"

################################################################################
# Create Sample List and Directories
################################################################################

echo ""
echo -e "${YELLOW}Preparing for PBS array submission...${NC}"

mkdir -p "$TEMP_DIR"
mkdir -p "$OUTPUT_DIR"

# Create logs directory for PBS output files
LOGS_DIR="$OUTPUT_DIR/logs_${RUN_DATE}_wes_${PROJECT_NAME}"
mkdir -p "$LOGS_DIR"

# Save sample list for PBS array job
SAMPLE_LIST_FILE="$TEMP_DIR/sample_list.txt"
printf '%s\n' "${SAMPLES[@]}" > "$SAMPLE_LIST_FILE"
echo "Sample list saved to: $SAMPLE_LIST_FILE"

################################################################################
# Create PBS Array Wrapper Script
################################################################################

# This wrapper calls wes_pipline.sh with the specific sample based on array index
ARRAY_WRAPPER="$TEMP_DIR/wes_array_wrapper.pbs"

cat > "$ARRAY_WRAPPER" << WRAPPER_EOF
#!/usr/bin/env bash
#PBS -N wes_array_${PROJECT_NAME}
#PBS -l select=1:ncpus=${THREADS}:mem=${MEMORY_GB}gb
#PBS -l walltime=${PBS_WALLTIME}
#PBS -q ${PBS_QUEUE}
#PBS -o ${SCRIPT_DIR}/wes_array_${PROJECT_NAME}.log
#PBS -j oe
#PBS -V

################################################################################
# PBS Array Wrapper - Calls wes_pipline.sh for each sample
# This script is auto-generated by wes_submit.sh
################################################################################

echo "========================================"
echo "PBS Array Worker"
echo "========================================"
echo "Job ID: \$PBS_JOBID"
echo "Array Index: \$PBS_ARRAY_INDEX"
echo "Node: \$(hostname)"
echo "Started: \$(date)"
echo "========================================"

# Read sample name from list using array index
SAMPLE_LIST="${SAMPLE_LIST_FILE}"
SAMPLE=\$(sed -n "\$((PBS_ARRAY_INDEX + 1))p" "\$SAMPLE_LIST")

if [ -z "\$SAMPLE" ]; then
    echo "ERROR: No sample found for array index \$PBS_ARRAY_INDEX"
    exit 1
fi

echo "Processing sample: \$SAMPLE"
echo ""

# Set environment variables for wes_pipline.sh
export CONFIG_FILE="${CONFIG_FILE}"
export SINGLE_SAMPLE="\$SAMPLE"
export PBS_ARRAY_MODE="true"

# Change to scripts directory
cd "${SCRIPT_DIR}"

# Run the original pipeline for this sample
# The pipeline will detect SINGLE_SAMPLE and PBS_ARRAY_MODE env vars
bash "${WES_PIPELINE}"

EXIT_CODE=\$?

echo ""
echo "========================================"
if [ \$EXIT_CODE -eq 0 ]; then
    echo "SUCCESS: \$SAMPLE"
else
    echo "FAILED: \$SAMPLE (exit code: \$EXIT_CODE)"
fi
echo "Finished: \$(date)"
echo "========================================"

exit \$EXIT_CODE
WRAPPER_EOF

chmod +x "$ARRAY_WRAPPER"
echo "PBS array wrapper created: $ARRAY_WRAPPER"

################################################################################
# Submit PBS Array Job
################################################################################

echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Submitting PBS Array Job${NC}"
echo -e "${YELLOW}========================================${NC}"

# Calculate array range (0 to N-1)
ARRAY_END=$((TOTAL_SAMPLES - 1))
echo "Array range: 0-${ARRAY_END} (${TOTAL_SAMPLES} samples)"
echo "Queue: ${PBS_QUEUE}"
echo "Resources: ${THREADS} CPUs, ${MEMORY_GB}GB RAM per sample"
echo ""

# Submit array job
echo "Submitting: qsub -J 0-${ARRAY_END} $ARRAY_WRAPPER"
echo ""

ARRAY_JOB_ID=$(qsub -J 0-${ARRAY_END} "$ARRAY_WRAPPER" 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to submit PBS array job${NC}"
    echo "$ARRAY_JOB_ID"
    exit 1
fi

echo -e "${GREEN}PBS Array Job submitted: $ARRAY_JOB_ID${NC}"

################################################################################
# Create Finalization Job (Joint Genotyping + VQSR)
################################################################################

echo ""
echo -e "${YELLOW}Creating finalization job for joint genotyping...${NC}"

FINALIZE_SCRIPT="$TEMP_DIR/wes_finalize.pbs"

cat > "$FINALIZE_SCRIPT" << FINALIZE_EOF
#!/usr/bin/env bash
#PBS -N wes_finalize_${PROJECT_NAME}
#PBS -l select=1:ncpus=${THREADS}:mem=${MEMORY_GB}gb
#PBS -l walltime=${PBS_WALLTIME}
#PBS -q ${PBS_QUEUE}
#PBS -o ${SCRIPT_DIR}/wes_finalize_${PROJECT_NAME}.log
#PBS -j oe
#PBS -V

################################################################################
# WES Finalization - Joint Genotyping and VQSR
# This job runs after all sample array jobs complete
################################################################################

echo "========================================"
echo "WES Finalization Job"
echo "========================================"
echo "Job ID: \$PBS_JOBID"
echo "Node: \$(hostname)"
echo "Started: \$(date)"
echo "========================================"

cd "${SCRIPT_DIR}"

# Run wes_pipline.sh in finalization mode
# (no SINGLE_SAMPLE set = normal mode, will do joint genotyping)
export CONFIG_FILE="${CONFIG_FILE}"
export FINALIZE_ONLY="true"

# The pipeline already has gvcf files from array jobs
# We just need to run joint genotyping part
bash "${WES_PIPELINE}"

EXIT_CODE=\$?

echo ""
echo "========================================"
if [ \$EXIT_CODE -eq 0 ]; then
    echo "SUCCESS: Joint genotyping and VQSR complete"
else
    echo "FAILED: Joint genotyping (exit code: \$EXIT_CODE)"
fi
echo "Finished: \$(date)"
echo "========================================"

exit \$EXIT_CODE
FINALIZE_EOF

chmod +x "$FINALIZE_SCRIPT"
echo "Finalization script created: $FINALIZE_SCRIPT"

################################################################################
# Submit Finalization Job (with dependency on array job)
################################################################################

echo ""
echo -e "${YELLOW}Submitting finalization job...${NC}"

FINALIZE_JOB_ID=$(qsub -W depend=afterany:"${ARRAY_JOB_ID}" "$FINALIZE_SCRIPT" 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}WARNING: Failed to submit finalization job with dependency${NC}"
    echo "$FINALIZE_JOB_ID"
    echo ""
    echo "You can manually submit it after array jobs complete:"
    echo "  qsub $FINALIZE_SCRIPT"
else
    echo -e "${GREEN}Finalization job submitted: $FINALIZE_JOB_ID${NC}"
    echo "Will run after all sample array jobs complete."
fi

################################################################################
# Summary
################################################################################

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Submission Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Jobs Submitted:"
echo "  Sample Array Job: $ARRAY_JOB_ID"
echo "  Finalization Job: ${FINALIZE_JOB_ID:-'(submit manually)'}"
echo ""
echo "Total Samples: $TOTAL_SAMPLES"
echo "Processing on: ${PBS_QUEUE} queue"
echo ""
echo "Workflow:"
echo "  1. Array jobs process each sample in parallel (alignment, BQSR, GVCF)"
echo "  2. Finalization job runs joint genotyping + VQSR after all samples complete"
echo ""
echo "Monitor progress with:"
echo "  qstat -u \$USER                    # View all your jobs"
echo "  qstat -Jt ${ARRAY_JOB_ID}         # View array job details"
echo ""
echo "Sample List: $SAMPLE_LIST_FILE"
echo "Array Wrapper: $ARRAY_WRAPPER"
echo "Finalize Script: $FINALIZE_SCRIPT"
echo ""
echo "Output will be saved to: $OUTPUT_DIR"
echo "========================================="
