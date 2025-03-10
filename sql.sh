
time spark-shell  \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4 \
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-east-2:395965142574:bucket/dkpark-s3table-bucket-us-east-2 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions <<EOF



/*
spark.sql(""" SELECT * FROM s3tablesbucket.example_namespace.example_table order by 1 """).show()
spark.sql(""" SELECT * FROM s3tablesbucket.example_namespace.example_table2 order by 1 """).show()
spark.sql(""" select count(*) from s3tablesbucket.example_namespace.ttt """).show()
spark.sql("""
create table s3tablesbucket.example_namespace.vcf
as
#select * from s3tablesbucket.example_namespace.vcf_temp
""")
*/


spark.time { spark.sql(""" delete  from s3tablesbucket.example_namespace.vcf where sample_id ='HG04192' """) }


spark.sql(""" select count(*) from s3tablesbucket.example_namespace.vcf """).show()


spark.time {
spark.sql("""
merge into s3tablesbucket.example_namespace.vcf as tgt
using s3tablesbucket.example_namespace.vcf_tmp_hg04192 as src
on (tgt.sample_id = src.sample_id)
when not matched then
insert  (sample_id,	chrom,	pos,	id,	ref,	alt,	qual,	filter,	ac,	ad,	af,	an,	dp,	f1r2,	f2r1,	fractioninformativereads,	fs,	gp,	gq,	gt,	lod,	mb,	mq,	mqranksum,	pri,	ps,	qd,	r2_5p_bias,	readposranksum,	sb,	sor,	sq )
values(src.sample_id,	src.chrom,	src.pos,	src.id,	src.ref,	src.alt,	src.qual,	src.filter,	src.ac,	src.ad,	src.af,	src.an,	src.dp,	src.f1r2,	src.f2r1,	src.fractioninformativereads,	src.fs,	src.gp,	src.gq,	src.gt,	src.lod,	src.mb,	src.mq,	src.mqranksum,	src.pri,	src.ps,	src.qd,	src.r2_5p_bias,	src.readposranksum,	src.sb,	src.sor,	src.sq)
""")
}


spark.time { spark.sql(""" select sample_id,count(*) from s3tablesbucket.example_namespace.vcf group by sample_id """).show() }
spark.sql(""" select count(*) from s3tablesbucket.example_namespace.vcf """).show()


EOF
