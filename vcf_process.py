from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split, expr, concat_ws, explode, lit
import os,sys
import logging

from ins_s3table import merge_table

#base_s3dir="s3://test-bucket-us-east-2-395965142574/VCF/FILE/"
temp_s3dir="s3://vcf-bucket-395965142574/VCF/FILE1/"
out_s3dir="s3://vcf-bucket-395965142574/VCF/FILE3/"

def processing_one_file(s3filename):

    #s3filename="s3://1000genomes-dragen/data/dragen-3.5.7b/hg38_altaware_nohla-cnv-anchored/NA07051/NA07051.hard-filtered.vcf.gz"

    spark = SparkSession.builder.appName("VCFtoS3Tables").getOrCreate()

    filename=os.path.basename(s3filename);

    sample_id=filename.split(".",1)
    print(sample_id[0])
    
    logging.info('s3 read without delimeter ')
    #data = spark.read.option("delimiter","\u0000").csv("s3://test-bucket-us-east-2-395965142574/VCF/FILE/HG00263.hard-filtered.vcf.gz")
    data = spark.read.option("delimiter","\u0000").csv(s3filename)
    logging.info('s3 read without delimeter done')
   
    logging.info('head_data search')
    head_data=data.filter(data._c0.startswith("#CHROM"))
    logging.info('head_data search done')
    
    logging.info('data search start with #CHROM')
    data_data=data.filter(~(data._c0.startswith("#")))
    logging.info('data search start with #CHROM done')
    
    logging.info('head + data')
    union_data = head_data.union(data_data)
    logging.info('head + data done')
    
    union_data.show(truncate=False)
    

    logging.info('Data for realdata S3 write')
    union_data.coalesce(1). \
        write. \
        option("header", "false"). \
        option("quote","\u0000"). \
        mode("overwrite"). \
        csv(temp_s3dir+filename+".temp")
    logging.info('Data for realdata S3 write done')
    
    logging.info('Read data from S3 with tabbed format')
    data2 = spark.read.option("header","true").option("delimiter","\t").option("quote", '^').csv(temp_s3dir+filename+".temp")
    
    data2.show(truncate=False)
    data2.printSchema()
    logging.info('Read data from S3 with tabbed format done')
    
    
    logging.info('INFO, FORMAT, NA12 column value format to  a=1;b=1;c=1')

    df_split = data2.withColumn("keys", split(data2[8], ":")) \
                 .withColumn("values", split(data2[9], ":"))
    
    # Combine keys and values into key=value format
    df_result = df_split.withColumn("result", expr("""
        concat_ws(';', transform(keys, (k, i) -> concat(k, '=', values[i])))
    """))
    
    df_result_all = df_result.withColumn("one_column", expr(""" concat_ws(';', INFO,result) """))

    
    df_split = df_result_all.withColumn("kv_pairs", split(df_result_all.one_column, ";")).withColumn("sample_id",lit(sample_id[0]))

    df_split.show(100,truncate=False)

    logging.info('INFO, FORMAT, NA12 column value format to  a=1;b=1;c=1 done')

    logging.info('explode a=1;b=1;c=1 to multiple rows')
    df_exploded = df_split.select(df_split["sample_id"],df_split[0],df_split[1],df_split[2],df_split[3],df_split[4],df_split[5],df_split[6], explode(df_split.kv_pairs).alias("kv_pair"))
    
    df_final = df_exploded.select(
        df_exploded[0],
        df_exploded[1],
        df_exploded[2],
        df_exploded[3],
        df_exploded[4],
        df_exploded[5],
        df_exploded[6],
        df_exploded[7],
        split(df_exploded.kv_pair, "=").getItem(0).alias("key"),
        split(df_exploded.kv_pair, "=").getItem(1).alias("value")
    )
    
    # Show the result
    df_final.show(100,truncate=False)

    logging.info('explode a=1;b=1;c=1 to multiple rows done')
    
    logging.info('transpose  multiple rows to multiple cols')
    transposed_df_1 = df_final.groupBy(df_final[0],df_final[1],df_final[2],df_final[3],df_final[4],df_final[5],df_final[6],df_final[7]).pivot("key").agg( { "Value":"first"})

    for col in transposed_df_1.columns:
        transposed_df_1 = transposed_df_1.withColumnRenamed(col,col.lower().replace("#",""))
    
    transposed_df_1.show(10,truncate=False)

#    transposed_df_1.write.format('parquet') \
        #            .mode('overwrite') \
        #    .save(out_s3dir+sample_id[0]+"/")
    logging.info('transpose  multiple rows to multiple cols done')
    logging.info('transposed dataframe to S3')

    transposed_df_1.write.option("header", True) \
            .option("delimiter","\t") \
            .mode('overwrite') \
            .csv(out_s3dir + sample_id[0] )

    logging.info('transposed dataframe to S3 done')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("매개변수 1개는  필수입니다.")
    else:
        print(sys.argv[1])
        # All files and directories ending with .txt and that don't begin with a dot:
        #files=glob.glob("./VCF/VCF/*.hard-filtered.vcf.gz")
        #files=glob.glob(sys.argv[1])
        #for a in files:

        # Configure the logger
        temp_filename=os.path.basename(sys.argv[1])
        logid=temp_filename.split(".",1)

        os.makedirs('./log', exist_ok=True)

        logging.basicConfig(
            filename=f'./log/{logid[0]}_app.log',  # Log file name
            level=logging.INFO,  # Logging level
            format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
        )

        logging.info('START')
        processing_one_file(sys.argv[1])
        merge_table(sys.argv[1],out_s3dir)
        logging.info('END')
