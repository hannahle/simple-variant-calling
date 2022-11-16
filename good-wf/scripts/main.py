from wf import _build_index, align_reads, convert_to_bam, sort_bam, variant_calling
from latch.types import LatchFile, LatchDir

# align_reads(ref_genome_dir = LatchDir("latch:///wgs/ref_genome/"), read1 = LatchFile("latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq"), read2 = LatchFile("latch:///wgs/trimmed_fastqs/SRR2584863_2.trim.sub.fastq"))

# convert_to_bam(sam = LatchFile("latch:///wgs/results/aligned.sam"))

# sort_bam(bam=LatchFile("latch:///wgs/results/aligned.bam"))

variant_calling(ref_genome=LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta"), sorted_bam=LatchFile("latch:///wgs/results/aligned.sorted.bam"))