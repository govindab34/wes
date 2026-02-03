# WES Pipeline Requirements

## Software Dependencies

### Core Bioinformatics Tools

#### BWA (Burrows-Wheeler Aligner)

- **Version**: 0.7.17 or higher
- **Purpose**: Read alignment to reference genome
- **Installation**: Via Spack, Conda, or source

  ```bash
  # Spack
  spack install bwa@0.7.17

  # Conda
  conda install -c bioconda bwa
  ```

#### SAMtools

- **Version**: 1.19.2 or higher
- **Purpose**: BAM/CRAM manipulation, indexing, statistics
- **Features Required**:
  - CRAM support (requires htslib)
  - Threading support
- **Installation**:

  ```bash
  # Spack
  spack install samtools@1.19.2

  # Conda
  conda install -c bioconda samtools
  ```

#### Java (OpenJDK)

- **Version**: 17.0.8 or higher
- **Purpose**: Run Picard and GATK
- **Memory**: Must support heap sizes up to 28GB
- **Installation**:

  ```bash
  # Spack
  spack install openjdk@17

  # System package manager
  dnf install java-17-openjdk
  ```

#### Picard

- **Version**: 3.1.1 or higher
- **Purpose**: BAM processing, sorting, duplicate marking
- **Format**: JAR file
- **Download**: https://github.com/broadinstitute/picard/releases
- **Installation**:

  ```bash
  # Spack
  spack install picard@3.1.1

  # Manual
  wget https://github.com/broadinstitute/picard/releases/download/3.1.1/picard.jar
  ```

#### GATK (Genome Analysis Toolkit)

- **Version**: 4.5.0.0 or higher
- **Purpose**: BQSR, variant calling, joint genotyping, VQSR
- **Format**: JAR file (gatk-package-\*-local.jar)
- **Download**: https://github.com/broadinstitute/gatk/releases
- **Installation**:

  ```bash
  # Spack
  spack install gatk@4.5.0.0

  # Manual
  wget https://github.com/broadinstitute/gatk/releases/download/4.5.0.0/gatk-4.5.0.0.zip
  unzip gatk-4.5.0.0.zip
  ```

### System Utilities

#### Bash

- **Version**: 4.0 or higher
- **Features**: Array support, process substitution

#### Standard Unix Tools

- `sed`, `awk`, `grep`, `find` - File processing
- `date`, `hostname` - System info
- `mkdir`, `rm`, `cp`, `mv` - File operations
- `tee` - Log output

### Optional Tools

#### Spack (Module System)

- **Purpose**: Software environment management
- **Setup Script**: Required if using Spack-installed tools
- **Path**: `/apps/spack/share/spack/setup-env.sh`

## Reference Data Requirements

### Reference Genome

- **Build**: GRCh38/hg38
- **File**: FASTA format with indices
- **Required Indices**:
  - `.fai` - SAMtools index
  - `.dict` - Picard sequence dictionary
  - `.amb`, `.ann`, `.bwt`, `.pac`, `.sa` - BWA indices
- **Download**:
  - NCBI: https://www.ncbi.nlm.nih.gov/genome/guide/human/
  - Broad: https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0

### Exome Capture BED File

- **Format**: BED (tab-delimited, sorted)
- **Columns**: chr, start, end (0-based coordinates)
- **Requirement**: Must match reference genome build
- **Example Sources**:
  - Twist Exome 2.0
  - Agilent SureSelect
  - IDT xGen

### Known Sites (BQSR)

- **dbSNP**: `Homo_sapiens_assembly38.dbsnp138.vcf.gz`
  - URL: https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0
- **Mills Indels**: `Mills_and_1000G_gold_standard.indels.hg38.vcf.gz`
  - URL: https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0
- **Required Indices**: `.tbi` (tabix index) for all VCF files

### VQSR Resources

- **HapMap**: `hapmap_3.3.hg38.vcf.gz`
- **1000 Genomes Omni**: `1000G_omni2.5.hg38.vcf.gz`
- **1000 Genomes SNPs**: `1000G_phase1.snps.high_confidence.hg38.vcf.gz`
- **dbSNP**: `Homo_sapiens_assembly38.dbsnp138.vcf.gz` (same as BQSR)
- **Mills Indels**: `Mills_and_1000G_gold_standard.indels.hg38.vcf.gz` (same as BQSR)
- **Required Indices**: `.tbi` for all VCF files
- **Download**: https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0

## System Requirements

### Compute Resources (Per Sample)

- **CPU Cores**: 16 (minimum), 32+ recommended
- **RAM**: 32 GB (minimum), 64 GB recommended
- **Disk Space**:
  - Input FASTQ: ~10-20 GB per sample (paired-end)
  - Temp space: ~50-100 GB per sample during processing
  - Output CRAM: ~5-10 GB per sample (compressed)
  - VCF: Varies with cohort size

### Storage Requirements

- **Input Directory**: Read access, SSD preferred
- **Temp Directory**: Read/write, fast I/O critical (SSD/NVMe recommended)
- **Output Directory**: Read/write, can be slower storage
- **Backup Directory**: Large capacity for archival

### HPC/Cluster Requirements

- **Job Scheduler**: PBS/Torque
  - Array job support (`qsub -J`)
  - Job dependencies (`qsub -W depend=afterany`)
- **Queue Configuration**:
  - CPU queue with sufficient walltime (24+ hours)
  - Node allocation with requested resources

## Software Versions Used (Tested)

| Tool     | Version    | Build/Source      |
| -------- | ---------- | ----------------- |
| BWA      | 0.7.17     | Spack GCC 11.2.0  |
| SAMtools | 1.19.2     | Spack GCC 13.2.0  |
| Java     | 17.0.8.1_1 | OpenJDK via Spack |
| Picard   | 3.1.1      | Spack GCC 13.2.0  |
| GATK     | 4.5.0.0    | Spack GCC 13.2.0  |

## Input Data Requirements

### FASTQ Files

- **Format**: Paired-end reads
- **Compression**: `.gz` (gzipped) or uncompressed
- **Naming Conventions** (supported):
  - `<sample>_1.clean.fastq.gz`, `<sample>_2.clean.fastq.gz`
  - `<sample>_1.trimmed.fastq.gz`, `<sample>_2.trimmed.fastq.gz`
  - `<sample>_1.fastq.gz`, `<sample>_2.fastq.gz`
  - `<sample>_R1.fastq.gz`, `<sample>_R2.fastq.gz`
- **Quality**: Pre-trimmed/cleaned (adapter removal, quality filtering)
- **Read Length**: Any (pipeline auto-detects)

### Quality Control

Pre-processing required:

1. Adapter trimming (Trimmomatic, fastp, etc.)
2. Quality filtering (Q20 or Q30 recommended)
3. Read length filtering (>50bp recommended)

## Environment Setup

### Example Spack Setup

```bash
# Load Spack environment
source /apps/spack/share/spack/setup-env.sh

# Load required packages
spack load bwa@0.7.17
spack load samtools@1.19.2
spack load openjdk@17
spack load picard@3.1.1
spack load gatk@4.5.0.0
```

### Example Module Setup (if using Environment Modules)

```bash
module load bwa/0.7.17
module load samtools/1.19.2
module load java/17
module load picard/3.1.1
module load gatk/4.5.0.0
```

### Environment Variables

The pipeline automatically sets:

- `PATH`: Adds BWA and SAMtools directories
- `SPACK_PYTHON`: Python for Spack (if Spack setup exists)
- `JVM_ARGS`: Java heap size based on config

## Validation

### Verify Installation

```bash
# Check BWA
bwa 2>&1 | head -n 3

# Check SAMtools
samtools --version

# Check Java
java -version

# Check Picard
java -jar /path/to/picard.jar -h

# Check GATK
java -jar /path/to/gatk.jar --list
```

### Verify Reference Files

```bash
# Check FASTA indices
ls -lh /path/to/reference.fa*

# Expected files:
# reference.fa
# reference.fa.fai
# reference.dict
# reference.fa.amb
# reference.fa.ann
# reference.fa.bwt
# reference.fa.pac
# reference.fa.sa

# Check VCF indices
ls -lh /path/to/dbsnp.vcf.gz*

# Expected files:
# dbsnp.vcf.gz
# dbsnp.vcf.gz.tbi
```

## Minimum vs Recommended Specs

| Resource         | Minimum  | Recommended | Notes                                |
| ---------------- | -------- | ----------- | ------------------------------------ |
| CPU Cores        | 16       | 32          | More cores = faster alignment        |
| RAM              | 32 GB    | 64 GB       | Prevents OOM errors                  |
| Temp Storage     | 100 GB   | 500 GB      | SSD/NVMe for performance             |
| Samples for VQSR | 10       | 30+         | Better VQSR models with more samples |
| Walltime         | 12 hours | 24 hours    | Varies with sample size              |

## Optional Performance Tools

- **GNU Parallel**: For batch processing optimization
- **tmux/screen**: For long-running interactive sessions
- **rsync**: For efficient data transfers
- **FastQC**: Pre-pipeline QC validation
- **MultiQC**: Aggregate QC report generation
