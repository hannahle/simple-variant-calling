"""
Simple Variant Calling Workflow
"""
import subprocess
import os
from pathlib import Path

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter, LatchDir
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import glob


@dataclass_json
@dataclass
class PairedReads:
    r1: LatchFile
    r2: LatchFile


@small_task
def build_index(
    ref_genome: LatchFile = LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta"),
) -> LatchDir:
    _bwa_cmd = ["bwa", "index", ref_genome.local_path]
    subprocess.run(_bwa_cmd)
    output = os.path.dirname(os.path.abspath(ref_genome.local_path))

    return LatchDir(output, "latch:///wgs/ref_genome")


# To-do: check if two reads are corrupted
@small_task
def align_reads(
    ref_genome_dir: LatchDir = LatchDir("latch:///wgs/ref_genome"),
    read1: LatchFile = LatchFile(
        "latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq"
    ),
    read2: LatchFile = LatchFile(
        "latch:///wgs/trimmed_fastqs/SRR2584863_2.trim.sub.fastq"
    ),
) -> LatchFile:

    local_ref_dir = ref_genome_dir.local_path

    fastas = glob.glob(f"{local_ref_dir}/*.fasta")

    sam_file = Path("aligned.sam").resolve()

    print(fastas, flush=True)

    if len(fastas) > 0:
        ref_genome = str(Path(fastas[0]).resolve())
        cmd = [
            "bwa",
            "mem",
            ref_genome,
            read1.local_path,
            read2.local_path,
            "-o",
            str(sam_file),
        ]
        subprocess.run(cmd)
    return LatchFile(str(sam_file), "latch:///wgs/results/aligned.sam")


@small_task
def convert_to_bam(
    sam: LatchFile = LatchFile("latch:///wgs/results/aligned.sam"),
) -> LatchFile:
    bam_file = Path("aligned.bam").resolve()
    _samtools_cmd = [
        "samtools",
        "view",
        "-S",
        "-b",
        sam.local_path,
        "-o",
        str(bam_file),
    ]

    subprocess.run(_samtools_cmd)

    return LatchFile(str(bam_file), "latch:///wgs/results/aligned.bam")


@small_task
def sort_bam(bam: LatchFile) -> LatchFile:
    sorted_bam = Path("aligned.sorted.bam").resolve()
    _sort_cmd = ["samtools", "sort", "-o", str(sorted_bam), bam.local_path]

    subprocess.run(_sort_cmd)

    return LatchFile(str(sorted_bam), "latch:///wgs/results/aligned.sorted.bam")


@small_task
def variant_calling(ref_genome: LatchFile, sorted_bam: LatchFile) -> LatchFile:

    # Calculate read coverage
    bcf = Path("raw.bcf").resolve()
    _read_coverage = [
        "bcftools",
        "mpileup",
        "-O",
        "b",
        "-o",
        str(bcf),
        "-f",
        ref_genome.local_path,
        sorted_bam.local_path,
    ]
    subprocess.run(_read_coverage)

    # Detect SNVs
    vcf = Path("variants.vcf").resolve()
    _snv_detection = [
        "bcftools",
        "call",
        "--ploidy",
        "1",
        "-m",
        "-v",
        "-o",
        str(vcf),
        str(bcf),
    ]
    subprocess.run(_snv_detection)

    # Filter and report the SNV variants in variant calling format (VCF)
    final_vcf = Path("final_variants.vcf").resolve()
    f = open(str(final_vcf), "w")
    _vcfutils_cmd = [
        "vcfutils.pl",
        "varFilter",
        str(vcf),
    ]
    subprocess.run(_vcfutils_cmd, stdout=f)

    return LatchFile(str(final_vcf), "latch:///wgs/results/final_variants.vcf")


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Simple WGS",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="John von Neumann",
        email="hungarianpapi4@gmail.com",
        github="github.com/fluid-dynamix",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "read1": LatchParameter(
            display_name="Read 1",
            description="Paired-end read 1 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "read2": LatchParameter(
            display_name="Read 2",
            description="Paired-end read 2 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "ref_genome": LatchParameter(display_name="Reference Genome"),
    },
    tags=[],
)


@workflow(metadata)
def wgs(
    ref_genome: LatchFile = LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta"),
    read1: LatchFile = LatchFile(
        "latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq"
    ),
    read2: LatchFile = LatchFile(
        "latch:///wgs/trimmed_fastqs/SRR2584863_2.trim.sub.fastq"
    ),
) -> LatchFile:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2
    """
    ref_genome_dir = build_index(ref_genome=ref_genome)
    sam = align_reads(ref_genome_dir=ref_genome_dir, read1=read1, read2=read2)
    bam = convert_to_bam(sam=sam)
    sorted_bam = sort_bam(bam=bam)
    return variant_calling(ref_genome=ref_genome, sorted_bam=sorted_bam)


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    wgs,
    "Test Data",
    {
        "ref_genome": LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta"),
        "read1": LatchFile("latch:///wgs/data/SRR2584863_1.trim.sub.fastq"),
        "read2": LatchFile("latch:///wgs/data/SRR2584863_2.trim.sub.fastq"),
    },
)
