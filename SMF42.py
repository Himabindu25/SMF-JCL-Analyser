import pandas
import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns


class SMF42:
    spark = SparkSession.builder.appName("SMF Data").getOrCreate()

    pdf = pandas.read_excel('file:///C:/Users/M1055990/Desktop/SMF Files/File extracted/File Monitoring smf 21 '
                            'Arranged.xlsx', sheet_name='Sheet1')

    schema = StructType([
        StructField('SYS', StringType(), True),
        StructField('DATE', DateType(), True),
        StructField('Time', StringType(), True),
        StructField('SUB_TYPE', StringType(), True),
        StructField('OFFSET', IntegerType(), True),
        StructField('LENGTH', IntegerType(), True),
        StructField('NO_PRODUCT', IntegerType(), True),
        StructField('OFFSET TO MEMBER DELETE', IntegerType(), True),
        StructField('LENGTH OF MEMBER DELETE', IntegerType(), True),
        StructField('SMF42LN4', IntegerType(), True),
        StructField('JOB NAME', StringType(), True),
        StructField('STEP NAME', StringType(), True),
        StructField('DATASET NAME', StringType(), True),
        StructField('VOL SER', StringType(), True),
        StructField('LENGTH_OF_MEMBER', IntegerType(), True),
        StructField('FLAG BIT', IntegerType(), True),
        StructField('MEMBER_NAME', StringType(), True)
    ])

    df = spark.createDataFrame(pdf, schema=schema)
    # df.show()

    writer = pandas.ExcelWriter("C:\\Users\\M1055990\\Desktop\\SMF_Data\\SMF42Data_.xls",
                                engine='xlsxwriter')
    df.toPandas().to_excel(writer, sheet_name='SMF Data')

    Data_df = df.groupBy("DATASET NAME").agg(F.count("DATASET NAME").alias('DATASET COUNT'))
    # Data_df.show()
    # Data_df.toPandas().to_excel(writer, sheet_name='Data set')

    Unit_df = df.groupBy("VOL SER").agg(F.count("VOL SER").alias('UNIT COUNT'))
    # Unit_df.show()
    # Unit_df.toPandas().to_excel(writer, sheet_name='Unit')

    count_df = df.groupBy("DATASET NAME", "MEMBER_NAME").agg(F.count("MEMBER_NAME").alias('Member Count'))
    count_df.show()
    max_min_countdf = count_df.groupBy("DATASET NAME", "MEMBER_NAME").agg(F.max("Member Count").alias('MAX COUNT'),
                                                                          F.min("Member Count").alias('Min Count'))
    max_min_countdf.show()

    df1 = df.groupBy("DATASET NAME").agg(F.max("MEMBER_NAME").alias('Max Member Count'),
                                         F.min("MEMBER_NAME").alias('Min Member Count'))
    df1.show()
    df1.toPandas().to_excel(writer, sheet_name='MAX MIN MEMBER')
    max_min_countdf.show()
    df1.show()

    count_df.toPandas().to_excel(writer, sheet_name='Data_Unit Count')
    max_min_countdf.toPandas().to_excel(writer, sheet_name='Max Min Dataset Count')

    Jobname_df = df.groupBy("JOB NAME", "STEP NAME").agg(F.count("STEP NAME").alias('STEP COUNT'))
    Jobname_df.show()
    Jobname_df.toPandas().to_excel(writer, sheet_name='Job_step count')
    # writer.save()

    # def token(x):
    #     v = re.search(".(\w+).", x)
    #     if v is None:
    #         return "None"
    #     else:
    #         return v.group(0 )
    #     # # r = re.findall(r'([\w\s]*[\?\!\.;])', x)
    #     # # return r
    #     # r = re.compile(r"(?:(?<=\s)|^)(?:[a-z]|\d+)", re.I)
    #     # return ''.join(r.findall(x))
    #
    #
    # udffun = F.udf(token, returnType=StringType())
    # df2 = df.withColumn("Dataset Token", udffun("DATASET NAME"))
    # df2.show()

    jobdf = Jobname_df.toPandas()
    max_min = max_min_countdf.toPandas()
    df1 = df1.toPandas()

    # The below code will create two plots. The parameters that .subplot take are (row, column, no. of plots).
    plt.subplot(2, 1, 1)
    plt.bar(jobdf["JOB NAME"], jobdf["STEP COUNT"], data=jobdf, color='yellow', label='Job Name', edgecolor='red')
    plt.legend(loc='upper right', title='SMF Info')
    plt.ylabel('Count')
    plt.xticks(
        rotation=20,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('JOB Name')

    plt.subplot(2, 1, 2)
    plt.bar(jobdf["STEP NAME"], jobdf["STEP COUNT"], data=jobdf, color='green', label='Step Name', edgecolor='blue')
    plt.legend(loc='upper right', title='SMF Info')
    plt.ylabel('Count')
    plt.xticks(
        rotation=45,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('STEP Name')
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\subplot_stepname.png")

    plt.clf()

    plt.subplot(2, 1, 1)
    plt.bar(max_min["DATASET NAME"], max_min["MAX COUNT"], color='yellow', label='DATASET NAME', edgecolor='red')
    plt.legend(loc='upper right', title='SMF Info')
    plt.ylabel('Max Count')
    plt.xticks(
        rotation=90,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('DATASET NAME')

    plt.clf()
    plt.subplot(2, 1, 2)
    plt.bar(max_min["DATASET NAME"], max_min["Min Count"], color='yellow', label='DATASET NAME', edgecolor='red')
    plt.legend(loc='upper right', title='SMF Info')
    plt.ylabel('Min Count')
    plt.xticks(
        rotation=90,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('Dataset Name')
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\subplot_datasetcount.png")

    plt.clf()
    plt.bar(df1["DATASET NAME"], df1["Min Member Count"], color='green', label='DATASET NAME', edgecolor='red')
    plt.legend(loc='upper right', title='SMF Info')
    plt.ylabel('Member Name')
    plt.xticks(
        rotation=90,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('Dataset Name')
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\min_memebercount.png")
    # plt.show()


if __name__ == '__main__':
    enabler = SMF42()
