from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
import sys
import glob
import logging



def get_full_ddl(df, table_name, location, file_format):
    columns = ','.join([f"{field.name} {field.dataType.simpleString()}\n" for field in df.schema])
    columns_new=columns.replace("#","")
    columns_new_lower=columns_new.lower()
#    columns_new_trim=columns_new_lower.strip()
    ddl = f"""CREATE TABLE IF NOT EXISTS s3tables.vcf_namespace.{table_name} ({columns_new_lower}
    )
    USING iceberg 
    """
    return ddl

def merge_table(in_file,out_s3dir):
    spark = SparkSession.builder.appName('iceberg_lab') \
        .config('spark.sql.defaultCatalog', 's3tables') \
        .config('spark.sql.catalog.s3tables', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.s3tables.client.region', 'ap-northeast-2') \
        .config('spark.sql.catalog.s3tables.warehouse', 'arn:aws:s3tables:ap-northeast-2:395965142574:bucket/vcf-s3tables-bucket-ap-northeast-2') \
        .config('spark.sql.catalog.s3tables.catalog-impl', 'software.amazon.s3tables.iceberg.S3TablesCatalog') \
        .enableHiveSupport() \
        .getOrCreate()

    in_file_name=os.path.basename(in_file)
    file_name=in_file_name.split(".")
    
    logging.info('spark read ')
    df = spark.read.option("delimiter","\t").csv(out_s3dir+file_name[0]+"/", header=True,inferSchema=False )
    #df = spark.read.parquet("s3://test-bucket-us-east-2-395965142574/VCF/FILE3/"+file_name[0]+"/" )
    logging.info('spark read done')
    
    tmp_table = os.path.basename(in_file).split(".")
    tmp2_table = tmp_table[0].lower()
    temp_table = "vcf_tmp_" + tmp2_table
    #spark.sql(""" delete from  s3tables.vcf_namespace.vcf_temp  """)
    #spark.sql(""" DROP TABLE  s3tables.vcf_namespace.vcf_temp purge """)

#
    #table_new_ddl=''
    #table_new_ddl=get_full_ddl(df,temp_table,temp_table,'csv')
    #print(table_new_ddl)
    #spark.sql(table_new_ddl)
#
    logging.info('s3 table insert')
    
    df.writeTo("s3tables.vcf_namespace."+temp_table).using("Iceberg").tableProperty ("format-version", "2").createOrReplace()

    logging.info('s3 table insert done ')
    logging.info('s3 table merge  ')
    
    spark.sql(f"""
              MERGE INTO s3tables.vcf_namespace.vcf as tgt
              USING s3tables.vcf_namespace.{temp_table} as src
              ON (tgt.sample_id = src.sample_id)
              WHEN NOT MATCHED THEN
                  INSERT (sample_id,     chrom,  pos,    id,     ref,    alt,    qual,   filter, ac,     ad,     af,     an,     dp,     f1r2,   f2r1,   fractioninformativereads,       fs,     gp,     gq,     gt,     lod,    mb,     mq,     mqranksum,      pri,    ps,     qd,     r2_5p_bias,     readposranksum, sb,     sor,    sq )
                  VALUES (src.sample_id,   src.chrom,      src.pos,        src.id, src.ref,        src.alt,        src.qual,       src.filter,     src.ac, src.ad, src.af, src.an, src.dp, src.f1r2,       src.f2r1,       src.fractioninformativereads,   src.fs, src.gp, src.gq, src.gt, src.lod,        src.mb, src.mq, src.mqranksum,  src.pri,        src.ps, src.qd, src.r2_5p_bias, src.readposranksum,     src.sb, src.sor,        src.sq)
              """)
    
    logging.info('s3 table merge done')

    spark.sql(f""" select sample_id, count(*) as cnt from s3tables.vcf_namespace.{temp_table} group by sample_id """).show()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("매개변수 1개는  필수입니다.")
    else:
        #print(sys.argv[1])
        # All files and directories ending with .txt and that don't begin with a dot:
        #files=glob.glob("./VCF/VCF/*.hard-filtered.vcf.gz")
#        files=glob.glob(sys.argv[1])
#        for a in files:
#            print(a)
        print(sys.argv[1])
        merge_table(sys.argv[1],"s3://vcf-bucket-395965142574/VCF/FILE3/")
