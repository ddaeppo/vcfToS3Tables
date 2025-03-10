## 프로그램

sudo yum install git -y

cd /mnt

git clone https://github.com/ddaeppo/vcfToS3Tables.git

cd vcfToS3Tables/

chmod +x *.sh

pip install -r requirements.txt

## vcf_process.py 화일의 아래내용 수정
temp_s3dir="s3://vcf-bucket-395965142574/VCF/FILE1/"
out_s3dir="s3://vcf-bucket-395965142574/VCF/FILE3/"

## 프로그램 수행
./spark.sh vcf_process.py s3://1000genomes-dragen/data/dragen-3.5.7b/hg38_altaware_nohla-cnv-anchored/additional_698_related/NA19983/NA19983.hard-filtered.vcf.gz
