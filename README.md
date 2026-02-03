# WES Pipeline - Whole Exome Sequencing Analysis

A comprehensive, production-ready pipeline for processing Whole Exome Sequencing (WES) data from trimmed FASTQ files through alignment, variant calling, and VQSR filtering.

## Features

- **YAML-based Configuration**: Centralized configuration for all paths and parameters
- **PBS Array Job Support**: Parallel processing of multiple samples using HPC job arrays
- **GATK Best Practices**: Follows GATK best practices for WES variant calling
- **Automatic Backup**: Backs up previous runs before starting new analysis
- **Comprehensive QC**: Generates alignment and coverage statistics
- **CRAM Output**: Space-efficient CRAM format for final alignments
- **Joint Genotyping**: Multi-sample joint variant calling with VQSR filtering
- **Cleanup Management**: Automatic cleanup of intermediate files

## Pipeline Workflow

### Per-Sample Processing (Array Jobs)

1. **Alignment**: BWA-MEM alignment with reference genome
2. **BAM Processing**:
   - Fix mate information
   - Sort by coordinate
   - Mark duplicates
3. **QC Statistics**: Generate alignment and coverage metrics
4. **CRAM Conversion**: Convert to CRAM format for storage
5. **BQSR**: Base Quality Score Recalibration
6. **Variant Calling**: HaplotypeCaller in GVCF mode

### Multi-Sample Processing (Finalization Job)

7. **Joint Genotyping**: CombineGVCFs + GenotypeGVCFs
8. **VQSR Filtering**: Variant Quality Score Recalibration for SNPs and INDELs
9. **Final VCF**: High-quality, filtered variant calls

## Directory Structure

```
wes/
├── config.yaml           # Main configuration file
├── wes_submit.sh         # PBS array job submission script
├── wes_pipline.sh        # Main pipeline script
├── README.md             # This file
└── REQUIREMENTS.md       # Tool and dependency requirements
```

### Output Structure

```
output/
├── cram_YYYY_MM_DD_wes_<project>/    # Final CRAM files + reference
├── vcf_YYYY_MM_DD_wes_<project>/     # Final VCF files
├── logs_YYYY_MM_DD_wes_<project>/    # Execution logs
└── qc_stats_YYYY_MM_DD_wes_<project>/# QC statistics
```

## Quick Start

### 1. Configure the Pipeline

Edit `config.yaml` to set your paths:

```yaml
directories:
  input: "/path/to/cleaned/fastq"
  output: "/path/to/output"
  temp: "/path/to/temp"

reference:
  fasta: "/path/to/GRCh38.fa"
  bed: "/path/to/exome_targets.bed"
```

### 2. Prepare Input Data

Place cleaned FASTQ files in the input directory with one of these naming patterns:

- `*_1.clean.fastq.gz` and `*_2.clean.fastq.gz`
- `*_1.trimmed.fastq.gz` and `*_2.trimmed.fastq.gz`
- `*_1.fastq.gz` and `*_2.fastq.gz`

### 3. Submit the Pipeline

```bash
# Submit with default config.yaml
./wes_submit.sh

# Or specify a config file
./wes_submit.sh /path/to/config.yaml
```

The submission script will:

- Detect all samples
- Backup any existing data
- Submit a PBS array job for parallel sample processing
- Submit a finalization job for joint genotyping

### 4. Monitor Progress

```bash
# View all your jobs
qstat -u $USER

# View array job details
qstat -Jt <ARRAY_JOB_ID>

# Check logs
tail -f wes_array_<project>.log
```

## Configuration

### Key Parameters

#### Processing Resources

```yaml
processing:
  batch_size: 5 # Samples processed simultaneously
  threads: 16 # CPU cores per sample
  memory_gb: 32 # RAM allocation per sample
  jvm_memory_gb: 28 # Java heap size (90% of memory)
```

#### PBS Settings

```yaml
pbs:
  queue: "cpu"
  walltime: "24:00:00"
```

#### BWA Parameters

```yaml
bwa:
  use_soft_clipping: true
  chunk_size: 50000000 # Memory-efficient processing
```

## Input Requirements

- **FASTQ Files**: Paired-end, cleaned/trimmed FASTQ files (gzipped or uncompressed)
- **Reference Genome**: GRCh38 FASTA with indices (.fai, .dict, .amb, .ann, .bwt, .pac, .sa)
- **Target BED**: Exome capture regions (sorted and validated)
- **Known Sites**: dbSNP, Mills indels for BQSR
- **VQSR Resources**: HapMap, 1000G, Omni for variant filtering

## Output Files

### Per Sample

- `<sample>.dedup.cram` - Final aligned reads
- `<sample>.dedup.cram.crai` - CRAM index
- `<sample>.flagstat.txt` - Alignment statistics
- `<sample>.coverage.txt` - Coverage metrics
- `<sample>.depth_summary.txt` - Depth statistics
- `<sample>.log` - Processing log

### Multi-Sample

- `cohort.vcf.gz` - Raw joint-called variants
- `cohort.snps.recal.vcf.gz` - SNPs after VQSR
- `cohort.indels.recal.vcf.gz` - INDELs after VQSR
- `cohort.final.vcf.gz` - Final filtered variants

## Advanced Usage

### Manual Sample Processing

Process a single sample without PBS:

```bash
export CONFIG_FILE="config.yaml"
export SINGLE_SAMPLE="sample_name"
bash wes_pipline.sh
```

### Resume Failed Runs

If array jobs fail for some samples:

1. Check which samples failed in logs
2. Manually resubmit specific array indices:
   ```bash
   qsub -J 2,5,7 wes_array_wrapper.pbs
   ```

## Backup and Recovery

- **Auto-backup**: Previous runs are automatically backed up to `backup/backup_YYYY_MM_DD/`
- **Backup includes**: CRAM files, VCF files, logs, QC stats, reference
- **Recovery**: Copy files from backup directory to restore previous results

## Performance Optimization

For 16 cores, 32GB RAM per sample:

- **Alignment**: ~2-4 hours per sample
- **Variant Calling**: ~1-2 hours per sample
- **Joint Genotyping**: Depends on cohort size (1-6 hours for 10-100 samples)
- **Total**: ~4-8 hours for complete single-sample processing

Scale by:

- Increasing `threads` and `memory_gb` for more resources
- Adjusting `batch_size` for parallel sample processing
- Using faster storage (SSD/NVMe) for temp directory

## Troubleshooting

### Common Issues

1. **Missing FASTQ files**
   - Check input directory path
   - Verify file naming pattern matches supported formats

2. **Out of memory errors**
   - Increase `memory_gb` and `jvm_memory_gb`
   - Reduce `batch_size`
   - Decrease `max_records_in_ram` in config

3. **BWA alignment fails**
   - Verify reference genome indices exist
   - Check BWA version compatibility
   - Review `<sample>.bwa.log` for errors

4. **VQSR fails**
   - Ensure sufficient sample count (≥30 recommended)
   - Check VQSR resource files are properly indexed
   - Adjust `max_gaussians` for small cohorts

## Citation

If you use this pipeline, please cite:

- **GATK**: McKenna A, et al. The Genome Analysis Toolkit: a MapReduce framework for analyzing next-generation DNA sequencing data. Genome Res. 2010
- **BWA**: Li H. and Durbin R. Fast and accurate short read alignment with Burrows-Wheeler Transform. Bioinformatics. 2009
- **Picard**: http://broadinstitute.github.io/picard

## License

This pipeline is provided as-is for research purposes.

## Support

For issues or questions:

- Check logs in `logs_*/` directory
- Review GATK documentation: https://gatk.broadinstitute.org
- Contact: govindab34

## Version History

- **v1.0** - Initial release with PBS array support and GATK best practices
