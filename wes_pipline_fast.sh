#!/usr/bin/env bash
#PBS -N wes_pipeline_fast
#PBS -q cpu
#PBS -l select=1:ncpus=100:mem=100gb
#PBS -l walltime=08:00:00

################################################################################
# WES Pipeline - Configurable Batch Processing
#
# Usage: qsub wes_pipeline.sh --config config.yaml
#
# This pipeline processes whole exome sequencing data from cleaned FASTQ files
# through alignment, BAM processing, BQSR, variant calling, and VQSR filtering.
#
# Features:
# - YAML-based configuration
# - Batch processing of samples
# - Temporary folder management with cleanup
# - Comprehensive logging
# - Final output: CRAM files, VCF files, logs only
################################################################################

# Note: Using set -u only (not -e) to allow proper error handling in parallel jobs
set -u

################################################################################
# Configuration File Setup
################################################################################
#
# USAGE:
#   1. Edit CONFIG_FILE path below to point to your config.yaml
#   2. Run: qsub wes_pipeline.sh
#
# OR use: qsub -v CONFIG_FILE=/path/to/config.yaml wes_pipeline.sh
#
################################################################################

# ============================================================================
# SET YOUR CONFIG FILE PATH HERE (edit this line!)
# ============================================================================
DEFAULT_CONFIG="/home/govind.mangropa/config_fast.yaml"
# ============================================================================

# Check if CONFIG_FILE was passed via qsub -v, otherwise use default
if [ -z "${CONFIG_FILE:-}" ]; then
    CONFIG_FILE="$DEFAULT_CONFIG"
fi

# Also check PBS_O_WORKDIR for config.yaml
if [ ! -f "$CONFIG_FILE" ] && [ -n "${PBS_O_WORKDIR:-}" ] && [ -f "${PBS_O_WORKDIR}/config.yaml" ]; then
    CONFIG_FILE="${PBS_O_WORKDIR}/config.yaml"
fi

echo "Using configuration file: $CONFIG_FILE"

# Validate config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file not found: $CONFIG_FILE"
    echo ""
    echo "Please either:"
    echo "  1. Edit the DEFAULT_CONFIG path in this script (line ~40)"
    echo "  2. Or use: qsub -v CONFIG_FILE=/your/path/config.yaml $0"
    echo ""
    exit 1
fi

echo "========================================"
echo "WES Pipeline Starting"
echo "========================================"
echo "Job ID: ${PBS_JOBID:-INTERACTIVE}"
echo "Started: $(date)"
echo "Config: $CONFIG_FILE"
echo "Working Directory: ${PBS_O_WORKDIR:-$(pwd)}"
echo "========================================"

cd "${PBS_O_WORKDIR:-$(pwd)}"

################################################################################
# YAML Parser Function (Simple)
################################################################################

parse_yaml() {
    local yaml_file=$1
    local prefix=${2:-}
    local s='[[:space:]]*'
    local w='[a-zA-Z0-9_]*'
    local fs=$(echo @|tr @ '\034')

    sed -ne "s|^\($s\):|\1|" \
         -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
         -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p" "$yaml_file" |
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
echo "Loading configuration..."

# Parse YAML and export variables
eval $(parse_yaml "$CONFIG_FILE" "CONFIG_")

# Core directories
INPUT_DIR="${CONFIG_directories_input}"
OUTPUT_DIR="${CONFIG_directories_output}"
TEMP_DIR="${CONFIG_directories_temp}"

# Processing parameters
BATCH_SIZE="${CONFIG_processing_batch_size:-5}"
THREADS="${CONFIG_processing_threads:-16}"
MEMORY_GB="${CONFIG_processing_memory_gb:-32}"
JVM_MEM="${CONFIG_processing_jvm_memory_gb:-28}"

# Reference files
REF_FASTA="${CONFIG_reference_fasta}"
WES_BED="${CONFIG_reference_bed}"

# Known sites
KNOWN_SNPS="${CONFIG_known_sites_snps}"
KNOWN_INDELS="${CONFIG_known_sites_indels}"

# VQSR resources
HAPMAP="${CONFIG_vqsr_resources_hapmap}"
OMNI="${CONFIG_vqsr_resources_omni}"
KG_SNPS="${CONFIG_vqsr_resources_kg_snps}"
DBSNP="${CONFIG_vqsr_resources_dbsnp}"
MILLS="${CONFIG_vqsr_resources_mills}"

# Tool paths
BWA_PATH="${CONFIG_tools_bwa}"
SAMTOOLS_PATH="${CONFIG_tools_samtools}"
JAVA_BIN="${CONFIG_tools_java}"
PICARD_JAR="${CONFIG_tools_picard}"
GATK_JAR="${CONFIG_tools_gatk}"
SPACK_SETUP="${CONFIG_tools_spack_setup}"

# BWA parameters
BWA_CHUNK_SIZE="${CONFIG_bwa_chunk_size:-50000000}"

# Cleanup config
DELETE_TEMP="${CONFIG_cleanup_delete_temp_on_success:-true}"

# Project settings
PROJECT_NAME="${CONFIG_project_name:-wes_project}"
RUN_DATE=$(date '+%Y_%m_%d_%a')  # Format: 2025_12_15_Sun

# Export all key variables for use in functions
export INPUT_DIR OUTPUT_DIR TEMP_DIR
export BATCH_SIZE THREADS MEMORY_GB JVM_MEM JVM_ARGS
export REF_FASTA WES_BED KNOWN_SNPS KNOWN_INDELS
export HAPMAP OMNI KG_SNPS DBSNP MILLS
export BWA_PATH SAMTOOLS_PATH JAVA_BIN PICARD_JAR GATK_JAR
export BWA_CHUNK_SIZE PROJECT_NAME RUN_DATE

echo "Configuration loaded successfully!"
echo "  Project Name: $PROJECT_NAME"
echo "  Run Date: $RUN_DATE"
echo "  Input Directory: $INPUT_DIR"
echo "  Output Directory: $OUTPUT_DIR"
echo "  Temp Directory: $TEMP_DIR"
echo "  Batch Size: $BATCH_SIZE samples"
echo "  Threads: $THREADS"
echo "  Memory: ${MEMORY_GB}GB (JVM: ${JVM_MEM}GB)"
echo "  BWA Chunk Size: $BWA_CHUNK_SIZE"

################################################################################
# Setup Environment
################################################################################

echo ""
echo "Setting up environment..."

# Fix Python for Spack
if [ -x "/usr/bin/python3" ]; then
    export SPACK_PYTHON=/usr/bin/python3
elif [ -x "/usr/bin/python3.6" ]; then
    export SPACK_PYTHON=/usr/bin/python3.6
elif [ -x "/usr/bin/python3.8" ]; then
    export SPACK_PYTHON=/usr/bin/python3.8
fi

# Load Spack if setup script exists
if [ -f "$SPACK_SETUP" ]; then
    source "$SPACK_SETUP"
    spack unload -a
    spack load python 2>/dev/null || true
fi

# Add tools to PATH
export PATH="${BWA_PATH}:${SAMTOOLS_PATH}:${PATH}"

# Verify Java
if [ ! -x "$JAVA_BIN" ]; then
    echo "ERROR: Java not found at: $JAVA_BIN"
    exit 1
fi
echo "Java version:"
"$JAVA_BIN" -version

# Expand Picard JAR path (handles wildcards)
PICARD_JAR=$(ls $PICARD_JAR 2>/dev/null | head -n 1)
if [ ! -f "$PICARD_JAR" ]; then
    echo "ERROR: Picard JAR not found: ${CONFIG_tools_picard}"
    exit 1
fi
echo "Picard JAR: $PICARD_JAR"

# Expand GATK JAR path
GATK_JAR=$(ls $GATK_JAR 2>/dev/null | head -n 1)
if [ ! -f "$GATK_JAR" ]; then
    echo "ERROR: GATK JAR not found: ${CONFIG_tools_gatk}"
    exit 1
fi
echo "GATK JAR: $GATK_JAR"

JVM_ARGS="-Xmx${JVM_MEM}g"

################################################################################
# Backup Previous Run (if any)
################################################################################

# Skip backup in PBS array mode or FINALIZE_ONLY mode (managed by wes_submit.sh)
if [ -n "${SINGLE_SAMPLE:-}" ] || [ -n "${FINALIZE_ONLY:-}" ]; then
    echo ""
    echo "PBS Array/Finalize Mode: Skipping backup (managed by wes_submit.sh)"
else

echo ""
echo "Checking for existing data to backup..."

# Backup directory location (from config or default)
BACKUP_BASE_DIR="${CONFIG_directories_backup:-$(dirname "$OUTPUT_DIR")/backup}"
BACKUP_DIR="$BACKUP_BASE_DIR/backup_${RUN_DATE}"

# Check if there's anything to backup
BACKUP_NEEDED=false

if [ -d "$OUTPUT_DIR" ]; then
    # Count files in each category
    CRAM_COUNT=$(find "$OUTPUT_DIR" -name "*.cram" 2>/dev/null | wc -l)
    VCF_COUNT=$(find "$OUTPUT_DIR" -name "*.vcf.gz" 2>/dev/null | wc -l)
    LOG_COUNT=$(find "$OUTPUT_DIR" -name "*.log" 2>/dev/null | wc -l)
    QC_COUNT=$(find "$OUTPUT_DIR" -path "*/qc_stats*" -name "*.txt" 2>/dev/null | wc -l)
    REF_EXISTS=false

    # Check for reference files (in output root or cram folder)
    if [ -f "$OUTPUT_DIR/reference.fa" ] || [ -L "$OUTPUT_DIR/reference.fa" ]; then
        REF_EXISTS=true
    else
        for cram_dir in "$OUTPUT_DIR"/cram_*; do
            if [ -f "$cram_dir/reference.fa" ] || [ -L "$cram_dir/reference.fa" ]; then
                REF_EXISTS=true
                break
            fi
        done
    fi

    if [ "$CRAM_COUNT" -gt 0 ] || [ "$VCF_COUNT" -gt 0 ] || [ "$LOG_COUNT" -gt 0 ] || [ "$QC_COUNT" -gt 0 ]; then
        BACKUP_NEEDED=true
    fi
fi

if [ "$BACKUP_NEEDED" = "true" ]; then
    echo "Found existing data to backup:"
    echo "  CRAM files: $CRAM_COUNT"
    echo "  VCF files: $VCF_COUNT"
    echo "  Log files: $LOG_COUNT"
    echo "  QC stat files: $QC_COUNT"
    echo "  Reference exists: $REF_EXISTS"
    echo ""
    echo "Creating backup at: $BACKUP_DIR"

    # Create backup subdirectories
    mkdir -p "$BACKUP_DIR/cram"
    mkdir -p "$BACKUP_DIR/vcf"
    mkdir -p "$BACKUP_DIR/logs"
    mkdir -p "$BACKUP_DIR/qc_stats"

    # Backup CRAM files
    if [ "$CRAM_COUNT" -gt 0 ]; then
        echo "  Backing up CRAM files..."
        find "$OUTPUT_DIR" -name "*.cram" -exec cp {} "$BACKUP_DIR/cram/" \; 2>/dev/null || true
        find "$OUTPUT_DIR" -name "*.cram.crai" -exec cp {} "$BACKUP_DIR/cram/" \; 2>/dev/null || true
    fi

    # Backup reference files to cram folder
    if [ "$REF_EXISTS" = "true" ]; then
        echo "  Backing up reference files to cram folder..."
        if [ -f "$OUTPUT_DIR/reference.fa" ] || [ -L "$OUTPUT_DIR/reference.fa" ]; then
            cp -L "$OUTPUT_DIR/reference.fa" "$BACKUP_DIR/cram/" 2>/dev/null || true
            cp -L "$OUTPUT_DIR/reference.fa.fai" "$BACKUP_DIR/cram/" 2>/dev/null || true
        fi
        for cram_dir in "$OUTPUT_DIR"/cram_*; do
            if [ -f "$cram_dir/reference.fa" ] || [ -L "$cram_dir/reference.fa" ]; then
                cp -L "$cram_dir/reference.fa" "$BACKUP_DIR/cram/" 2>/dev/null || true
                cp -L "$cram_dir/reference.fa.fai" "$BACKUP_DIR/cram/" 2>/dev/null || true
                break
            fi
        done
    fi

    # Backup VCF files
    if [ "$VCF_COUNT" -gt 0 ]; then
        echo "  Backing up VCF files..."
        find "$OUTPUT_DIR" -name "*.vcf.gz" -exec cp {} "$BACKUP_DIR/vcf/" \; 2>/dev/null || true
        find "$OUTPUT_DIR" -name "*.vcf.gz.tbi" -exec cp {} "$BACKUP_DIR/vcf/" \; 2>/dev/null || true
    fi

    # Backup log files
    if [ "$LOG_COUNT" -gt 0 ]; then
        echo "  Backing up log files..."
        find "$OUTPUT_DIR" -name "*.log" -exec cp {} "$BACKUP_DIR/logs/" \; 2>/dev/null || true
        find "$OUTPUT_DIR" -name "*.err" -exec cp {} "$BACKUP_DIR/logs/" \; 2>/dev/null || true
    fi

    # Backup QC stats
    if [ "$QC_COUNT" -gt 0 ]; then
        echo "  Backing up QC statistics..."
        find "$OUTPUT_DIR" -path "*/qc_stats*" -name "*.txt" -exec cp {} "$BACKUP_DIR/qc_stats/" \; 2>/dev/null || true
    fi

    echo ""
    echo "Backup complete: $BACKUP_DIR"
    echo "  - cram/: CRAM files + reference"
    echo "  - vcf/: VCF files + indices"
    echo "  - logs/: Log and error files"
    echo "  - qc_stats/: QC statistics"
else
    echo "No existing data to backup. Skipping backup step."
fi

################################################################################
# Clean Previous Run (if any)
################################################################################

echo ""
echo "Cleaning previous run data..."

# Clear temp directory
if [ -d "$TEMP_DIR" ]; then
    echo "  Removing temp directory: $TEMP_DIR"
    rm -rf "$TEMP_DIR"
fi

# Clear output directory
if [ -d "$OUTPUT_DIR" ]; then
    echo "  Removing output directory: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi

echo "Cleanup complete!"

fi  # End of "if not SINGLE_SAMPLE" block for backup/cleanup

################################################################################
# Create Directory Structure
################################################################################

echo ""
echo "Creating directory structure..."

# Create dated folder names with project name
# Format: logs_YYYYMMDD_wes_projectname
LOGS_DIR="$OUTPUT_DIR/logs_${RUN_DATE}_wes_${PROJECT_NAME}"
CRAM_DIR="$OUTPUT_DIR/cram_${RUN_DATE}_wes_${PROJECT_NAME}"
VCF_DIR="$OUTPUT_DIR/vcf_${RUN_DATE}_wes_${PROJECT_NAME}"
QC_DIR="$OUTPUT_DIR/qc_stats_${RUN_DATE}_wes_${PROJECT_NAME}"

mkdir -p "$OUTPUT_DIR"
mkdir -p "$TEMP_DIR"
mkdir -p "$LOGS_DIR"
mkdir -p "$CRAM_DIR"
mkdir -p "$VCF_DIR"
mkdir -p "$QC_DIR"
mkdir -p "$TEMP_DIR/bam"
mkdir -p "$TEMP_DIR/gvcf"

# Export folder paths for use in worker scripts
export LOGS_DIR CRAM_DIR VCF_DIR QC_DIR

LOG_DIR="$LOGS_DIR"
MASTER_LOG="$LOG_DIR/pipeline_master.log"

# Export for background subshells
export LOG_DIR MASTER_LOG

# Start master log
{
    echo "========================================"
    echo "WES Pipeline Master Log"
    echo "Started: $(date)"
    echo "Job ID: ${PBS_JOBID:-INTERACTIVE}"
    echo "Config: $CONFIG_FILE"
    echo "========================================"
} > "$MASTER_LOG"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$MASTER_LOG"
}

log_message "Directory structure created"
log_message "Logs: $LOG_DIR"
log_message "Temp: $TEMP_DIR"
log_message "Output: $OUTPUT_DIR"

################################################################################
# Verify Required Files
################################################################################

log_message "Verifying required files..."

check_file() {
    if [ ! -f "$1" ]; then
        log_message "ERROR: Required file not found: $1"
        exit 1
    fi
}

check_file "$REF_FASTA"
check_file "$WES_BED"
check_file "$KNOWN_SNPS"
check_file "$KNOWN_INDELS"
check_file "$HAPMAP"
check_file "$OMNI"
check_file "$KG_SNPS"
check_file "$DBSNP"
check_file "$MILLS"

log_message "All required files verified!"

################################################################################
# Detect Input FASTQ Files
################################################################################

log_message "Detecting input FASTQ files..."

if [ ! -d "$INPUT_DIR" ]; then
    log_message "ERROR: Input directory not found: $INPUT_DIR"
    exit 1
fi

cd "$INPUT_DIR"

# Try multiple common FASTQ naming patterns
FASTQ_PATTERN=""
FASTQ_SUFFIX=""

# Pattern 1: *_1.clean.fastq.gz
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.clean.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_1.clean.fastq.gz"
        FASTQ_SUFFIX="_1.clean.fastq.gz"
        R2_SUFFIX="_2.clean.fastq.gz"
    fi
fi

# Pattern 2: *_1.trimmed.fastq.gz
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.trimmed.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_1.trimmed.fastq.gz"
        FASTQ_SUFFIX="_1.trimmed.fastq.gz"
        R2_SUFFIX="_2.trimmed.fastq.gz"
    fi
fi

# Pattern 3: *_1.trimmed.fastq (uncompressed)
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.trimmed.fastq" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_1.trimmed.fastq"
        FASTQ_SUFFIX="_1.trimmed.fastq"
        R2_SUFFIX="_2.trimmed.fastq"
    fi
fi

# Pattern 4: *_1.fastq.gz
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_1.fastq.gz"
        FASTQ_SUFFIX="_1.fastq.gz"
        R2_SUFFIX="_2.fastq.gz"
    fi
fi

# Pattern 5: *_1.fastq (uncompressed)
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_1.fastq" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_1.fastq"
        FASTQ_SUFFIX="_1.fastq"
        R2_SUFFIX="_2.fastq"
    fi
fi

# Pattern 6: *_R1*.fastq.gz
if [ -z "$FASTQ_PATTERN" ]; then
    mapfile -t R1_FILES < <(find . -maxdepth 1 -name "*_R1*.fastq.gz" -type f 2>/dev/null | sort)
    if [ ${#R1_FILES[@]} -gt 0 ]; then
        FASTQ_PATTERN="_R1"
        # Special handling needed for R1/R2 pattern
    fi
fi

if [ -z "$FASTQ_PATTERN" ] || [ ${#R1_FILES[@]} -eq 0 ]; then
    log_message "ERROR: No FASTQ files found in $INPUT_DIR"
    log_message "Supported patterns: *_1.clean.fastq.gz, *_1.trimmed.fastq.gz, *_1.trimmed.fastq, *_1.fastq.gz, *_1.fastq"
    exit 1
fi

log_message "Detected FASTQ pattern: $FASTQ_PATTERN"

# Extract sample names
SAMPLES=()
for R1 in "${R1_FILES[@]}"; do
    SAMPLE=$(basename "$R1" "$FASTQ_SUFFIX")
    SAMPLES+=("$SAMPLE")
done

TOTAL_SAMPLES=${#SAMPLES[@]}
log_message "Found $TOTAL_SAMPLES samples to process"
log_message "Samples: ${SAMPLES[*]}"

################################################################################
# PBS Array Mode Support
################################################################################
# When called from wes_submit.sh with PBS array job, SINGLE_SAMPLE is set
# In this mode, process only the specified sample instead of all samples

if [ -n "${SINGLE_SAMPLE:-}" ]; then
    log_message ""
    log_message "=========================================="
    log_message "PBS ARRAY MODE: Processing single sample"
    log_message "=========================================="
    log_message "Sample: $SINGLE_SAMPLE"
    
    # Verify the sample exists in our detected samples
    SAMPLE_FOUND=false
    for s in "${SAMPLES[@]}"; do
        if [ "$s" = "$SINGLE_SAMPLE" ]; then
            SAMPLE_FOUND=true
            break
        fi
    done
    
    if [ "$SAMPLE_FOUND" = "false" ]; then
        log_message "ERROR: Sample '$SINGLE_SAMPLE' not found in input directory"
        log_message "Available samples: ${SAMPLES[*]}"
        exit 1
    fi
    
    # Override to process only this sample
    SAMPLES=("$SINGLE_SAMPLE")
    TOTAL_SAMPLES=1
    
    # In array mode, skip backup/cleanup (done once by first job or submit script)
    # Also skip joint genotyping (will be done by finalization job)
    PBS_ARRAY_MODE="${PBS_ARRAY_MODE:-false}"
    log_message "Array mode: $PBS_ARRAY_MODE"
fi

# Export for use in functions and background subshells
export FASTQ_SUFFIX R2_SUFFIX PICARD_JAR GATK_JAR JVM_ARGS

cd "${PBS_O_WORKDIR:-$(pwd)}"

################################################################################
# Helper Functions
################################################################################

# NOTE: All functions below are exported with 'export -f' at the end of their
# definitions so they can be called from background subshells (&)

# Function to process a single sample through alignment and BAM processing
process_sample_alignment() {
    local SAMPLE=$1
    local SAMPLE_LOG="$LOG_DIR/${SAMPLE}.log"
    local SAMPLE_ERR="$LOG_DIR/${SAMPLE}.err"

    {
        echo "========================================"
        echo "Processing Sample: $SAMPLE"
        echo "Started: $(date)"
        echo "========================================"

        # Input files (using detected pattern)
        R1="$INPUT_DIR/${SAMPLE}${FASTQ_SUFFIX}"
        R2="$INPUT_DIR/${SAMPLE}${R2_SUFFIX}"

        if [ ! -f "$R1" ]; then
            echo "ERROR: Missing R1 file: $R1"
            return 1
        fi

        if [ ! -f "$R2" ]; then
            echo "ERROR: Missing R2 file for $SAMPLE"
            return 1
        fi

        # Output files in temp
        RAW_BAM="$TEMP_DIR/bam/${SAMPLE}.raw.bam"
        FIXED_BAM="$TEMP_DIR/bam/${SAMPLE}.fixedmate.bam"
        SORTED_BAM="$TEMP_DIR/bam/${SAMPLE}.sorted.bam"
        DEDUP_BAM="$TEMP_DIR/bam/${SAMPLE}.dedup.bam"
        DEDUP_METRICS="$TEMP_DIR/bam/${SAMPLE}.dedup_metrics.txt"
        FINAL_CRAM="$OUTPUT_DIR/cram/${SAMPLE}.dedup.cram"

        # Read group string (properly escaped for BWA)
        RG_STRING="@RG\\tID:${SAMPLE}\\tSM:${SAMPLE}\\tPL:ILLUMINA\\tLB:${SAMPLE}_lib"

        echo ""
        echo "=== Step 1: BWA Alignment ==="
        echo "Start: $(date)"
        echo "DEBUG: R1=$R1"
        echo "DEBUG: R2=$R2"
        echo "DEBUG: REF=$REF_FASTA"
        echo "DEBUG: THREADS=$THREADS"
        echo "DEBUG: CHUNK_SIZE=$BWA_CHUNK_SIZE"
        echo "DEBUG: RG_STRING=$RG_STRING"
        echo "DEBUG: OUTPUT=$RAW_BAM"

        # Run BWA alignment - write SAM to file, errors to separate log
        SAM_TEMP="$TEMP_DIR/bam/${SAMPLE}.temp.sam"
        BWA_LOG="$LOG_DIR/${SAMPLE}.bwa.log"

        echo "Running BWA mem..."
        bwa mem \
            -Y \
            -K ${BWA_CHUNK_SIZE:-50000000} \
            -t ${THREADS:-16} \
            -R "$RG_STRING" \
            "$REF_FASTA" \
            "$R1" \
            "$R2" > "$SAM_TEMP" 2> "$BWA_LOG"

        BWA_EXIT=$?

        if [ $BWA_EXIT -ne 0 ]; then
            echo "ERROR: BWA mem failed for $SAMPLE (exit code: $BWA_EXIT)"
            echo "BWA output:"
            head -50 "$SAM_TEMP"
            rm -f "$SAM_TEMP"
            return 1
        fi

        # Check if SAM file has actual alignment data
        if ! grep -q "^@" "$SAM_TEMP" 2>/dev/null; then
            echo "ERROR: BWA produced no valid SAM output for $SAMPLE"
            echo "SAM file contents:"
            head -20 "$SAM_TEMP"
            rm -f "$SAM_TEMP"
            return 1
        fi

        echo "Converting SAM to BAM..."
        samtools view -Shb -@ ${THREADS:-16} -o "$RAW_BAM" "$SAM_TEMP"
        SAM_EXIT=$?

        # Cleanup temp SAM
        rm -f "$SAM_TEMP"

        if [ $SAM_EXIT -ne 0 ]; then
            echo "ERROR: samtools view failed for $SAMPLE (exit code: $SAM_EXIT)"
            return 1
        fi

        # Verify BAM was created and has content
        if [ ! -s "$RAW_BAM" ]; then
            echo "ERROR: BAM file is empty or not created for $SAMPLE"
            return 1
        fi

        echo "Complete: $(date)"
        echo "BAM size: $(du -h "$RAW_BAM" | cut -f1)"

        echo ""
        echo "=== Step 2: Fix Mate Information ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" FixMateInformation \
            MAX_RECORDS_IN_RAM=2000000 \
            VALIDATION_STRINGENCY=SILENT \
            ADD_MATE_CIGAR=True \
            ASSUME_SORTED=false \
            R="$REF_FASTA" \
            I="$RAW_BAM" \
            O="$FIXED_BAM"

        if [ $? -ne 0 ]; then
            echo "ERROR: FixMateInformation failed for $SAMPLE"
            return 1
        fi
        rm -f "$RAW_BAM"  # Clean up immediately
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 3: Sort BAM ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" SortSam \
            MAX_RECORDS_IN_RAM=2000000 \
            VALIDATION_STRINGENCY=SILENT \
            SORT_ORDER=coordinate \
            CREATE_INDEX=true \
            R="$REF_FASTA" \
            I="$FIXED_BAM" \
            O="$SORTED_BAM"

        if [ $? -ne 0 ]; then
            echo "ERROR: SortSam failed for $SAMPLE"
            return 1
        fi
        rm -f "$FIXED_BAM"  # Clean up immediately
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 4: Mark Duplicates ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" MarkDuplicates \
            MAX_RECORDS_IN_RAM=2000000 \
            VALIDATION_STRINGENCY=SILENT \
            M="$DEDUP_METRICS" \
            R="$REF_FASTA" \
            I="$SORTED_BAM" \
            O="$DEDUP_BAM"

        if [ $? -ne 0 ]; then
            echo "ERROR: MarkDuplicates failed for $SAMPLE"
            return 1
        fi
        rm -f "$SORTED_BAM"  # Clean up immediately

        # Index the deduplicated BAM (required for GATK interval-based processing)
        echo "Indexing deduplicated BAM..."
        samtools index -@ $THREADS "$DEDUP_BAM"
        if [ $? -ne 0 ]; then
            echo "ERROR: BAM indexing failed for $SAMPLE"
            return 1
        fi
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 5: Generate QC Statistics ==="
        echo "Start: $(date)"

        QC_DIR="$OUTPUT_DIR/qc_stats"

        # Flagstat - alignment statistics
        echo "Running samtools flagstat..."
        samtools flagstat -@ $THREADS "$DEDUP_BAM" > "$QC_DIR/${SAMPLE}.flagstat.txt"
        if [ $? -ne 0 ]; then
            echo "WARNING: flagstat failed for $SAMPLE (continuing anyway)"
        fi

        # Coverage - per-chromosome coverage stats
        echo "Running samtools coverage..."
        samtools coverage "$DEDUP_BAM" > "$QC_DIR/${SAMPLE}.coverage.txt"
        if [ $? -ne 0 ]; then
            echo "WARNING: coverage failed for $SAMPLE (continuing anyway)"
        fi

        # Depth statistics - mean depth calculation
        echo "Calculating depth statistics..."
        samtools depth -a "$DEDUP_BAM" | awk '{sum+=$3; count++} END {print "Mean Depth: " sum/count}' > "$QC_DIR/${SAMPLE}.depth_summary.txt"

        # Also save detailed depth per region (using BED file)
        echo "Calculating target region depth..."
        samtools depth -b "$WES_BED" "$DEDUP_BAM" | awk '{sum+=$3; count++} END {print "Target Region Mean Depth: " sum/count "\nTotal Bases Covered: " count}' >> "$QC_DIR/${SAMPLE}.depth_summary.txt"

        echo "QC files saved to: $QC_DIR/"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 6: Convert BAM to CRAM ==="
        echo "Start: $(date)"
        samtools view \
            -C \
            -T "$REF_FASTA" \
            -@ $THREADS \
            -o "$FINAL_CRAM" \
            "$DEDUP_BAM"

        if [ $? -ne 0 ]; then
            echo "ERROR: BAM to CRAM conversion failed for $SAMPLE"
            return 1
        fi

        echo "Indexing CRAM..."
        samtools index "$FINAL_CRAM"

        if [ $? -ne 0 ]; then
            echo "ERROR: CRAM indexing failed for $SAMPLE"
            return 1
        fi
        echo "Complete: $(date)"

        echo ""
        echo "========================================"
        echo "Sample $SAMPLE alignment & QC complete!"
        echo "CRAM: $FINAL_CRAM"
        echo "QC Stats: $OUTPUT_DIR/qc_stats/${SAMPLE}.*"
        echo "Finished: $(date)"
        echo "========================================"

    } >> "$SAMPLE_LOG" 2>> "$SAMPLE_ERR"

    return 0
}

# Function to process BQSR and generate GVCF
process_sample_variant_calling() {
    local SAMPLE=$1
    local SAMPLE_LOG="$LOG_DIR/${SAMPLE}.log"
    local SAMPLE_ERR="$LOG_DIR/${SAMPLE}.err"

    {
        echo ""
        echo "========================================"
        echo "Variant Calling: $SAMPLE"
        echo "Started: $(date)"
        echo "========================================"

        DEDUP_BAM="$TEMP_DIR/bam/${SAMPLE}.dedup.bam"
        RECAL_TABLE="$TEMP_DIR/bam/${SAMPLE}.recal_data.table"
        RECAL_BAM="$TEMP_DIR/bam/${SAMPLE}.recal.bam"
        GVCF_OUT="$TEMP_DIR/gvcf/${SAMPLE}.g.vcf.gz"

        echo ""
        echo "=== Step 6: Base Recalibration ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" BaseRecalibrator \
            -I "$DEDUP_BAM" \
            -R "$REF_FASTA" \
            --known-sites "$KNOWN_SNPS" \
            --known-sites "$KNOWN_INDELS" \
            -O "$RECAL_TABLE" \
            -L "$WES_BED" \
            --preserve-qscores-less-than 6

        if [ $? -ne 0 ]; then
            echo "ERROR: BaseRecalibrator failed for $SAMPLE"
            return 1
        fi
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 7: Apply BQSR ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" ApplyBQSR \
            -I "$DEDUP_BAM" \
            -R "$REF_FASTA" \
            --bqsr-recal-file "$RECAL_TABLE" \
            -O "$RECAL_BAM" \
            --preserve-qscores-less-than 6 \
            --static-quantized-quals 10 \
            --static-quantized-quals 20 \
            --static-quantized-quals 30

        if [ $? -ne 0 ]; then
            echo "ERROR: ApplyBQSR failed for $SAMPLE"
            return 1
        fi
        rm -f "$RECAL_TABLE"  # Clean up immediately
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 8: HaplotypeCaller (GVCF) ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" HaplotypeCaller \
            -R "$REF_FASTA" \
            -I "$RECAL_BAM" \
            -O "$GVCF_OUT" \
            -ERC GVCF \
            --sample-name "$SAMPLE" \
            -L "$WES_BED" \
            --annotation AlleleFraction \
            --annotation DepthPerAlleleBySample \
            --annotation Coverage \
            --annotation FisherStrand \
            --annotation MappingQualityRankSumTest \
            --annotation QualByDepth \
            --annotation ReadPosRankSumTest \
            --annotation RMSMappingQuality \
            --annotation StrandOddsRatio \
            --annotation InbreedingCoeff \
            --verbosity INFO

        if [ $? -ne 0 ]; then
            echo "ERROR: HaplotypeCaller failed for $SAMPLE"
            return 1
        fi

        # Clean up recalibrated BAM (keep dedup BAM for now)
        rm -f "$RECAL_BAM" "${RECAL_BAM%.bam}.bai"

        echo "Complete: $(date)"
        echo "GVCF created: $GVCF_OUT"

        echo ""
        echo "========================================"
        echo "Variant calling complete: $SAMPLE"
        echo "Finished: $(date)"
        echo "========================================"

    } >> "$SAMPLE_LOG" 2>> "$SAMPLE_ERR"

    return 0
}

################################################################################
# Main Processing Loop - GNU Parallel Processing
################################################################################

# Skip sample processing in FINALIZE_ONLY mode (jump to joint genotyping)
if [ -n "${FINALIZE_ONLY:-}" ]; then
    log_message ""
    log_message "========================================="
    log_message "FINALIZE ONLY MODE"
    log_message "========================================="
    log_message "Skipping sample processing (already done by array jobs)"
    log_message "Proceeding directly to joint genotyping..."
    
    # Set empty failed samples array (will check for gvcf files)
    FAILED_SAMPLES=()
else

log_message ""
log_message "========================================"
log_message "Starting GNU Parallel Processing"
log_message "Total Samples: $TOTAL_SAMPLES"
log_message "Parallel Jobs: $BATCH_SIZE samples at a time"
log_message "========================================"

# Create a temporary script that processes one sample
# This script will be called by GNU parallel for each sample
SAMPLE_SCRIPT="$TEMP_DIR/process_sample.sh"

cat > "$SAMPLE_SCRIPT" << 'SAMPLE_SCRIPT_EOF'
#!/usr/bin/env bash

# Arguments passed from parallel
SAMPLE="$1"
INPUT_DIR="$2"
OUTPUT_DIR="$3"
TEMP_DIR="$4"
REF_FASTA="$5"
WES_BED="$6"
KNOWN_SNPS="$7"
KNOWN_INDELS="$8"
THREADS="$9"
JVM_MEM="${10}"
JAVA_BIN="${11}"
PICARD_JAR="${12}"
GATK_JAR="${13}"
FASTQ_SUFFIX="${14}"
R2_SUFFIX="${15}"
BWA_CHUNK_SIZE="${16}"
LOGS_DIR="${17}"
CRAM_DIR="${18}"
VCF_DIR="${19}"
QC_DIR="${20}"

# Create per-sample directories
SAMPLE_LOG_DIR="$LOGS_DIR/$SAMPLE"
SAMPLE_QC_DIR="$QC_DIR/$SAMPLE"
mkdir -p "$SAMPLE_LOG_DIR"
mkdir -p "$SAMPLE_QC_DIR"

SAMPLE_LOG="$SAMPLE_LOG_DIR/${SAMPLE}.log"
SAMPLE_ERR="$SAMPLE_LOG_DIR/${SAMPLE}.err"
JVM_ARGS="-Xmx${JVM_MEM}g"
FAILED_FILE="$TEMP_DIR/failed_samples.txt"

# Define process_sample_alignment function
process_sample_alignment() {
    local SAMPLE=$1
    {
        echo "========================================"
        echo "Processing Sample: $SAMPLE"
        echo "Started: $(date)"
        echo "========================================"

        R1="$INPUT_DIR/${SAMPLE}${FASTQ_SUFFIX}"
        R2="$INPUT_DIR/${SAMPLE}${R2_SUFFIX}"

        if [ ! -f "$R1" ]; then
            echo "ERROR: Missing R1 file: $R1"
            return 1
        fi
        if [ ! -f "$R2" ]; then
            echo "ERROR: Missing R2 file for $SAMPLE"
            return 1
        fi

        RAW_BAM="$TEMP_DIR/bam/${SAMPLE}.raw.bam"
        FIXED_BAM="$TEMP_DIR/bam/${SAMPLE}.fixedmate.bam"
        SORTED_BAM="$TEMP_DIR/bam/${SAMPLE}.sorted.bam"
        DEDUP_BAM="$TEMP_DIR/bam/${SAMPLE}.dedup.bam"
        DEDUP_METRICS="$TEMP_DIR/bam/${SAMPLE}.dedup_metrics.txt"
        FINAL_CRAM="$CRAM_DIR/${SAMPLE}.dedup.cram"
        RG_STRING="@RG\\tID:${SAMPLE}\\tSM:${SAMPLE}\\tPL:ILLUMINA\\tLB:${SAMPLE}_lib"

        echo ""
        echo "=== Step 1: BWA Alignment ==="
        echo "Start: $(date)"
        SAM_TEMP="$TEMP_DIR/bam/${SAMPLE}.temp.sam"
        BWA_LOG="$SAMPLE_LOG_DIR/${SAMPLE}.bwa.log"

        bwa mem -Y -K ${BWA_CHUNK_SIZE} -t ${THREADS} -R "$RG_STRING" "$REF_FASTA" "$R1" "$R2" > "$SAM_TEMP" 2> "$BWA_LOG"
        if [ $? -ne 0 ]; then
            echo "ERROR: BWA mem failed for $SAMPLE"
            rm -f "$SAM_TEMP"
            return 1
        fi

        samtools view -Shb -@ ${THREADS} -o "$RAW_BAM" "$SAM_TEMP"
        rm -f "$SAM_TEMP"
        if [ ! -s "$RAW_BAM" ]; then
            echo "ERROR: BAM file is empty for $SAMPLE"
            return 1
        fi
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 2: Fix Mate Information ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" FixMateInformation \
            MAX_RECORDS_IN_RAM=2000000 VALIDATION_STRINGENCY=SILENT \
            ADD_MATE_CIGAR=True ASSUME_SORTED=false \
            R="$REF_FASTA" I="$RAW_BAM" O="$FIXED_BAM"
        if [ $? -ne 0 ]; then
            echo "ERROR: FixMateInformation failed for $SAMPLE"
            return 1
        fi
        rm -f "$RAW_BAM"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 3: Sort BAM ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" SortSam \
            MAX_RECORDS_IN_RAM=2000000 VALIDATION_STRINGENCY=SILENT \
            SORT_ORDER=coordinate CREATE_INDEX=true \
            R="$REF_FASTA" I="$FIXED_BAM" O="$SORTED_BAM"
        if [ $? -ne 0 ]; then
            echo "ERROR: SortSam failed for $SAMPLE"
            return 1
        fi
        rm -f "$FIXED_BAM"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 4: Mark Duplicates ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$PICARD_JAR" MarkDuplicates \
            MAX_RECORDS_IN_RAM=2000000 VALIDATION_STRINGENCY=SILENT \
            M="$DEDUP_METRICS" R="$REF_FASTA" I="$SORTED_BAM" O="$DEDUP_BAM"
        if [ $? -ne 0 ]; then
            echo "ERROR: MarkDuplicates failed for $SAMPLE"
            return 1
        fi
        rm -f "$SORTED_BAM"
        samtools index -@ $THREADS "$DEDUP_BAM"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 5: Generate QC Statistics ==="
        echo "Start: $(date)"
        samtools flagstat -@ $THREADS "$DEDUP_BAM" > "$SAMPLE_QC_DIR/${SAMPLE}.flagstat.txt" 2>/dev/null || true
        samtools coverage "$DEDUP_BAM" > "$SAMPLE_QC_DIR/${SAMPLE}.coverage.txt" 2>/dev/null || true
        samtools depth -a "$DEDUP_BAM" | awk '{sum+=$3; count++} END {print "Mean Depth: " sum/count}' > "$SAMPLE_QC_DIR/${SAMPLE}.depth_summary.txt"
        samtools depth -b "$WES_BED" "$DEDUP_BAM" | awk '{sum+=$3; count++} END {print "Target Region Mean Depth: " sum/count "\nTotal Bases Covered: " count}' >> "$SAMPLE_QC_DIR/${SAMPLE}.depth_summary.txt"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 6: Convert BAM to CRAM ==="
        echo "Start: $(date)"
        samtools view -C -T "$REF_FASTA" -@ $THREADS -o "$FINAL_CRAM" "$DEDUP_BAM"
        if [ $? -ne 0 ]; then
            echo "ERROR: BAM to CRAM conversion failed for $SAMPLE"
            return 1
        fi
        samtools index "$FINAL_CRAM"
        echo "Complete: $(date)"

        echo ""
        echo "Sample $SAMPLE alignment & QC complete!"
        echo "CRAM: $FINAL_CRAM"
    } >> "$SAMPLE_LOG" 2>> "$SAMPLE_ERR"
    return 0
}

# Define process_sample_variant_calling function
process_sample_variant_calling() {
    local SAMPLE=$1
    {
        echo ""
        echo "========================================"
        echo "Variant Calling: $SAMPLE"
        echo "Started: $(date)"
        echo "========================================"

        DEDUP_BAM="$TEMP_DIR/bam/${SAMPLE}.dedup.bam"
        RECAL_TABLE="$TEMP_DIR/bam/${SAMPLE}.recal_data.table"
        RECAL_BAM="$TEMP_DIR/bam/${SAMPLE}.recal.bam"
        GVCF_OUT="$TEMP_DIR/gvcf/${SAMPLE}.g.vcf.gz"

        echo ""
        echo "=== Step 7: Base Recalibration ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" BaseRecalibrator \
            -I "$DEDUP_BAM" -R "$REF_FASTA" \
            --known-sites "$KNOWN_SNPS" --known-sites "$KNOWN_INDELS" \
            -O "$RECAL_TABLE" -L "$WES_BED" --preserve-qscores-less-than 6
        if [ $? -ne 0 ]; then
            echo "ERROR: BaseRecalibrator failed for $SAMPLE"
            return 1
        fi
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 8: Apply BQSR ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" ApplyBQSR \
            -I "$DEDUP_BAM" -R "$REF_FASTA" --bqsr-recal-file "$RECAL_TABLE" \
            -O "$RECAL_BAM" --preserve-qscores-less-than 6 \
            --static-quantized-quals 10 --static-quantized-quals 20 --static-quantized-quals 30
        if [ $? -ne 0 ]; then
            echo "ERROR: ApplyBQSR failed for $SAMPLE"
            return 1
        fi
        rm -f "$RECAL_TABLE"
        echo "Complete: $(date)"

        echo ""
        echo "=== Step 9: HaplotypeCaller (GVCF) ==="
        echo "Start: $(date)"
        "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" HaplotypeCaller \
            -R "$REF_FASTA" -I "$RECAL_BAM" -O "$GVCF_OUT" -ERC GVCF \
            --sample-name "$SAMPLE" -L "$WES_BED" \
            --annotation AlleleFraction --annotation DepthPerAlleleBySample \
            --annotation Coverage --annotation FisherStrand \
            --annotation MappingQualityRankSumTest --annotation QualByDepth \
            --annotation ReadPosRankSumTest --annotation RMSMappingQuality \
            --annotation StrandOddsRatio --annotation InbreedingCoeff \
            --verbosity INFO
        if [ $? -ne 0 ]; then
            echo "ERROR: HaplotypeCaller failed for $SAMPLE"
            return 1
        fi
        rm -f "$RECAL_BAM" "${RECAL_BAM%.bam}.bai"
        echo "Complete: $(date)"
        echo "GVCF created: $GVCF_OUT"

        echo ""
        echo "Variant calling complete: $SAMPLE"
    } >> "$SAMPLE_LOG" 2>> "$SAMPLE_ERR"
    return 0
}

# Main execution
echo "Processing sample: $SAMPLE" >&2

if ! process_sample_alignment "$SAMPLE"; then
    echo "$SAMPLE" >> "$FAILED_FILE"
    echo "FAILED: $SAMPLE (alignment)" >&2
    exit 1
fi

if ! process_sample_variant_calling "$SAMPLE"; then
    echo "$SAMPLE" >> "$FAILED_FILE"
    echo "FAILED: $SAMPLE (variant calling)" >&2
    exit 1
fi

# Cleanup temp BAM files
rm -f "$TEMP_DIR/bam/${SAMPLE}.dedup.bam" 2>/dev/null
rm -f "$TEMP_DIR/bam/${SAMPLE}.dedup.bam.bai" 2>/dev/null
rm -f "$TEMP_DIR/bam/${SAMPLE}.dedup_metrics.txt" 2>/dev/null

echo "SUCCESS: $SAMPLE" >&2
exit 0
SAMPLE_SCRIPT_EOF

chmod +x "$SAMPLE_SCRIPT"
log_message "Created sample processing script: $SAMPLE_SCRIPT"

# Create failed samples tracking file
FAILED_FILE="$TEMP_DIR/failed_samples.txt"
> "$FAILED_FILE"

# Create sample list file
SAMPLE_LIST="$TEMP_DIR/sample_list.txt"
printf '%s\n' "${SAMPLES[@]}" > "$SAMPLE_LIST"
log_message "Sample list: $SAMPLE_LIST"

# Check if GNU Parallel is available
PARALLEL_CMD=""
if command -v parallel &> /dev/null; then
    PARALLEL_CMD="parallel"
elif [ -x "/usr/bin/parallel" ]; then
    PARALLEL_CMD="/usr/bin/parallel"
elif [ -x "/usr/local/bin/parallel" ]; then
    PARALLEL_CMD="/usr/local/bin/parallel"
fi

if [ -n "$PARALLEL_CMD" ]; then
    # Run with GNU Parallel
    log_message ""
    log_message "Launching GNU Parallel with $BATCH_SIZE concurrent jobs..."
    log_message "----------------------------------------"

    $PARALLEL_CMD -j "$BATCH_SIZE" --progress --joblog "$LOG_DIR/parallel_joblog.txt" \
        "$SAMPLE_SCRIPT" {} \
        "$INPUT_DIR" "$OUTPUT_DIR" "$TEMP_DIR" "$REF_FASTA" "$WES_BED" \
        "$KNOWN_SNPS" "$KNOWN_INDELS" "$THREADS" "$JVM_MEM" "$JAVA_BIN" \
        "$PICARD_JAR" "$GATK_JAR" "$FASTQ_SUFFIX" "$R2_SUFFIX" "$BWA_CHUNK_SIZE" \
        "$LOGS_DIR" "$CRAM_DIR" "$VCF_DIR" "$QC_DIR" \
        :::: "$SAMPLE_LIST"

    PARALLEL_EXIT=$?
    log_message "----------------------------------------"
    log_message "GNU Parallel completed with exit code: $PARALLEL_EXIT"
else
    # Fallback to sequential processing
    log_message ""
    log_message "WARNING: GNU Parallel not found. Running samples SEQUENTIALLY."
    log_message "To enable parallel processing, install GNU Parallel or add it to PATH."
    log_message "----------------------------------------"

    SAMPLE_COUNT=0
    for SAMPLE in "${SAMPLES[@]}"; do
        SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
        log_message ""
        log_message "Processing sample $SAMPLE_COUNT of $TOTAL_SAMPLES: $SAMPLE"

        "$SAMPLE_SCRIPT" "$SAMPLE" \
            "$INPUT_DIR" "$OUTPUT_DIR" "$TEMP_DIR" "$REF_FASTA" "$WES_BED" \
            "$KNOWN_SNPS" "$KNOWN_INDELS" "$THREADS" "$JVM_MEM" "$JAVA_BIN" \
            "$PICARD_JAR" "$GATK_JAR" "$FASTQ_SUFFIX" "$R2_SUFFIX" "$BWA_CHUNK_SIZE" \
            "$LOGS_DIR" "$CRAM_DIR" "$VCF_DIR" "$QC_DIR"

        if [ $? -ne 0 ]; then
            log_message "ERROR: Sample $SAMPLE failed"
        else
            log_message "SUCCESS: Sample $SAMPLE completed"
        fi
    done

    log_message "----------------------------------------"
    log_message "Sequential processing completed"
fi

# Read failed samples
FAILED_SAMPLES=()
if [ -f "$FAILED_FILE" ] && [ -s "$FAILED_FILE" ]; then
    while IFS= read -r sample; do
        FAILED_SAMPLES+=("$sample")
    done < "$FAILED_FILE"
fi

log_message ""
log_message "========================================"
log_message "All Samples Processed"
log_message "Total: $TOTAL_SAMPLES"
log_message "Failed: ${#FAILED_SAMPLES[@]}"
if [ ${#FAILED_SAMPLES[@]} -gt 0 ]; then
    log_message "Failed samples: ${FAILED_SAMPLES[*]}"
fi
log_message "========================================"

fi  # End of "if not FINALIZE_ONLY" block for sample processing

################################################################################
# Joint Genotyping and VQSR
################################################################################

# Skip joint genotyping in PBS array mode (handled by finalization job)
if [ -n "${SINGLE_SAMPLE:-}" ]; then
    log_message ""
    log_message "=========================================="
    log_message "PBS Array Mode: Skipping joint genotyping"
    log_message "=========================================="
    log_message "Joint genotyping will be handled by wes_submit.sh finalization job"
    log_message "after all samples complete."
    log_message ""
    log_message "Sample $SINGLE_SAMPLE processing complete!"
    log_message "CRAM: $CRAM_DIR/${SINGLE_SAMPLE}.dedup.cram"
    log_message "GVCF: $TEMP_DIR/gvcf/${SINGLE_SAMPLE}.g.vcf.gz"
    exit 0
fi

if [ ${#FAILED_SAMPLES[@]} -eq $TOTAL_SAMPLES ]; then
    log_message "ERROR: All samples failed. Skipping joint genotyping."
    exit 1
fi

log_message ""
log_message "========================================"
log_message "Starting Joint Genotyping"
log_message "========================================"

JOINT_LOG="$LOG_DIR/joint_genotyping.log"
JOINT_ERR="$LOG_DIR/joint_genotyping.err"

{
    echo "========================================"
    echo "Joint Genotyping and VQSR"
    echo "Started: $(date)"
    echo "========================================"

    COMBINED_GVCF="$TEMP_DIR/combined.g.vcf.gz"
    JOINT_VCF="$TEMP_DIR/joint_genotyped.vcf.gz"

    VQSR_SNP_RECAL="$TEMP_DIR/vqsr_snp.recal"
    VQSR_SNP_TRANCHES="$TEMP_DIR/vqsr_snp.tranches"
    VQSR_SNP_PLOTS="$TEMP_DIR/vqsr_snp_plots.R"

    VQSR_INDEL_RECAL="$TEMP_DIR/vqsr_indel.recal"
    VQSR_INDEL_TRANCHES="$TEMP_DIR/vqsr_indel.tranches"
    VQSR_INDEL_PLOTS="$TEMP_DIR/vqsr_indel_plots.R"

    VQSR_SNP_VCF="$TEMP_DIR/vqsr_snp.vcf.gz"
    FINAL_VCF="$VCF_DIR/final_filtered.vcf.gz"

    # Build GVCF input list
    GVCF_INPUTS=()
    for SAMPLE in "${SAMPLES[@]}"; do
        # Skip failed samples
        if [ ${#FAILED_SAMPLES[@]} -gt 0 ] && [[ " ${FAILED_SAMPLES[*]} " =~ " ${SAMPLE} " ]]; then
            continue
        fi
        GVCF="$TEMP_DIR/gvcf/${SAMPLE}.g.vcf.gz"
        if [ -f "$GVCF" ]; then
            GVCF_INPUTS+=("-V" "$GVCF")
        fi
    done

    if [ ${#GVCF_INPUTS[@]} -eq 0 ]; then
        echo "ERROR: No GVCF files found for joint genotyping"
        exit 1
    fi

    echo ""
    echo "=== Step 9: Combine GVCFs ==="
    echo "Start: $(date)"
    echo "Combining ${#GVCF_INPUTS[@]} GVCF files..."

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" CombineGVCFs \
        -R "$REF_FASTA" \
        "${GVCF_INPUTS[@]}" \
        -O "$COMBINED_GVCF"

    if [ $? -ne 0 ]; then
        echo "ERROR: CombineGVCFs failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Step 10: Joint Genotyping ==="
    echo "Start: $(date)"

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" GenotypeGVCFs \
        -R "$REF_FASTA" \
        -V "$COMBINED_GVCF" \
        -O "$JOINT_VCF"

    if [ $? -ne 0 ]; then
        echo "ERROR: GenotypeGVCFs failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Step 11: VQSR - SNPs ==="
    echo "Start: $(date)"

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" VariantRecalibrator \
        -R "$REF_FASTA" \
        -V "$JOINT_VCF" \
        --mode SNP \
        --output "$VQSR_SNP_RECAL" \
        --tranches-file "$VQSR_SNP_TRANCHES" \
        --rscript-file "$VQSR_SNP_PLOTS" \
        --resource:hapmap,known=false,training=true,truth=true,prior=15.0 "$HAPMAP" \
        --resource:omni,known=false,training=true,truth=true,prior=12.0 "$OMNI" \
        --resource:1000G,known=false,training=true,truth=false,prior=10.0 "$KG_SNPS" \
        --resource:dbsnp,known=true,training=false,truth=false,prior=2.0 "$DBSNP" \
        -an QD -an MQ -an FS -an MQRankSum -an ReadPosRankSum -an SOR \
        --tranche 100.0 --tranche 99.8 --tranche 99.6 --tranche 99.4 \
        --tranche 99.2 --tranche 99.0 --tranche 95.0 --tranche 90.0 \
        --dont-run-rscript

    if [ $? -ne 0 ]; then
        echo "ERROR: VariantRecalibrator SNP failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Step 12: VQSR - INDELs ==="
    echo "Start: $(date)"

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" VariantRecalibrator \
        -R "$REF_FASTA" \
        -V "$JOINT_VCF" \
        --mode INDEL \
        --output "$VQSR_INDEL_RECAL" \
        --tranches-file "$VQSR_INDEL_TRANCHES" \
        --rscript-file "$VQSR_INDEL_PLOTS" \
        --resource:mills,known=true,training=true,truth=true,prior=12.0 "$MILLS" \
        --resource:dbsnp,known=true,training=false,truth=false,prior=2.0 "$DBSNP" \
        -an QD -an FS -an ReadPosRankSum -an MQRankSum -an SOR \
        --tranche 100.0 --tranche 99.0 --tranche 95.0 --tranche 92.0 --tranche 90.0 \
        --max-gaussians 4 \
        --dont-run-rscript

    if [ $? -ne 0 ]; then
        echo "ERROR: VariantRecalibrator INDEL failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Step 13: Apply VQSR - SNPs ==="
    echo "Start: $(date)"

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" ApplyVQSR \
        -R "$REF_FASTA" \
        -V "$JOINT_VCF" \
        --mode SNP \
        --truth-sensitivity-filter-level 99.80 \
        --recal-file "$VQSR_SNP_RECAL" \
        --tranches-file "$VQSR_SNP_TRANCHES" \
        -O "$VQSR_SNP_VCF"

    if [ $? -ne 0 ]; then
        echo "ERROR: ApplyVQSR SNP failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Step 14: Apply VQSR - INDELs ==="
    echo "Start: $(date)"

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" ApplyVQSR \
        -R "$REF_FASTA" \
        -V "$VQSR_SNP_VCF" \
        --mode INDEL \
        --truth-sensitivity-filter-level 99.0 \
        --recal-file "$VQSR_INDEL_RECAL" \
        --tranches-file "$VQSR_INDEL_TRANCHES" \
        -O "$FINAL_VCF"

    if [ $? -ne 0 ]; then
        echo "ERROR: ApplyVQSR INDEL failed"
        exit 1
    fi
    echo "Complete: $(date)"

    echo ""
    echo "=== Variant Statistics ==="
    echo "Counting variants in final VCF..."

    "$JAVA_BIN" $JVM_ARGS -jar "$GATK_JAR" CountVariants \
        -V "$FINAL_VCF"

    echo ""
    echo "========================================"
    echo "Joint Genotyping Complete!"
    echo "Final VCF: $FINAL_VCF"
    echo "Finished: $(date)"
    echo "========================================"

} >> "$JOINT_LOG" 2>> "$JOINT_ERR"

if [ $? -ne 0 ]; then
    log_message "ERROR: Joint genotyping failed. Check $JOINT_ERR"
    exit 1
fi

log_message "Joint genotyping complete!"
log_message "Final VCF: $FINAL_VCF"

################################################################################
# Cleanup Temp Directory
################################################################################

if [ "$DELETE_TEMP" = "true" ]; then
    log_message ""
    log_message "========================================"
    log_message "Cleaning up temporary directory"
    log_message "========================================"

    # Keep GVCF files temporarily for verification, delete all else
    log_message "Removing temporary files..."
    rm -rf "$TEMP_DIR/bam"
    rm -f "$TEMP_DIR"/*.vcf.gz
    rm -f "$TEMP_DIR"/*.recal
    rm -f "$TEMP_DIR"/*.tranches
    rm -f "$TEMP_DIR"/*.R

    log_message "Temporary files cleaned up"
    log_message "GVCF files retained in: $TEMP_DIR/gvcf (delete manually if not needed)"
else
    log_message "Temp directory preserved: $TEMP_DIR"
fi

################################################################################
# Create Reference Link
################################################################################

log_message ""
log_message "Creating reference genome link in CRAM directory..."
ln -sf "$REF_FASTA" "$CRAM_DIR/reference.fa"
ln -sf "${REF_FASTA}.fai" "$CRAM_DIR/reference.fa.fai"
log_message "Reference linked: $CRAM_DIR/reference.fa"

################################################################################
# Final Summary
################################################################################

log_message ""
log_message "========================================"
log_message "WES Pipeline Complete!"
log_message "========================================"
log_message "Finished: $(date)"
log_message ""
log_message "Output Summary:"
log_message "  Project: $PROJECT_NAME"
log_message "  Run Date: $RUN_DATE"
log_message "  CRAM files: $CRAM_DIR/ (${TOTAL_SAMPLES} samples)"
log_message "  VCF files: $VCF_DIR/"
log_message "  QC Stats: $QC_DIR/"
log_message "  Logs: $LOGS_DIR/"
log_message "  Reference: $CRAM_DIR/reference.fa"
log_message ""
log_message "Statistics:"
log_message "  Total samples: $TOTAL_SAMPLES"
log_message "  Successful: $((TOTAL_SAMPLES - ${#FAILED_SAMPLES[@]}))"
log_message "  Failed: ${#FAILED_SAMPLES[@]}"
if [ ${#FAILED_SAMPLES[@]} -gt 0 ]; then
    log_message "  Failed samples: ${FAILED_SAMPLES[*]}"
fi
log_message "========================================"
