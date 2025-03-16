from datetime import datetime

from pyspark.sql import SparkSession

from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName('CreateDfFromRdd') \
    .master('local') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

# data = spark.sparkContext.parallelize([
#     Row(id=1, name="Thang", age=22),
#     Row(id=2, name="Nguyen Van Thang", age=23),
#     Row(id=3, name="nvThang", age=24),
# ])
# schema = StructType([
#     StructField('id', LongType(), True),
#     StructField('name', StringType(), True),
#     StructField('age', IntegerType(), True)
# ])
#
#
# df = spark.createDataFrame(data,schema)
# df.show()
# df.printSchema()

'''
SPARK SQL TYPE
StringType: chuỗi ký tư
LongType: Số nguyên 64bit
IntegerType: Số nguyên 32bit
FloatType: Số thập phân 32bit
DoubleType: Số thập phân 64bit
BooleanType: True/False
TimestampType: Ngày và giờ
DateType: Năm, tháng, ngày
DecimalType(precision, scale): Độ chính xác của số thập phân
    - precision: tổng chữ số
    - scale: số chữ số sau dấu phẩy
ByteType: Số nguyên 8 bit
ShortType: Số nguyên 16 bit
======ADVANCED======
StructType: Biểu diễn một cấu trúc
StructField(name, dataType, nullable): biểu diễn 1 trường trong StructType
    - name: Tên của trường
    - datatype: Kiểu dữ liệu của trường
    - nullable:
ArrayType(elementType): Biểu diễn các mảng được chỉ định
    - elementType: Kiểu dữ liệu của các phân tử trong mảng
MapType(keyType, valueType): Biểu diễn cặp khóa key-value
    - keyType: kiểu dữ liệu của key
    - valueType: kiểu dữ liệu của value
MapType
'''

data = [
    Row(
        name = 'Quang Anh Tran',
        age = 15,
        id = 1,
        salary = 10000.0,
        bonus = 5000.75,
        is_active=True,
        scores = [8, 8, 9],
        attributes = {'dept': 'Engineering', 'role': 'Data Engineer'},
        hire_date = datetime.strptime('2024-3-14', '%Y-%m-%d'),
        last_login = datetime.strptime('2024-3-14 22:33:14', '%Y-%m-%d %H:%M:%S')
    ),
    Row(
        name = 'Le Bao Hoang',
        age = 25,
        id = 2,
        salary = 20000.0,
        bonus = 1000.75,
        is_active = False,
        scores = [9, 8, 9],
        attributes = {'dept': 'Engineering', 'role': 'DevOps'},
        hire_date = datetime.strptime('2024-3-14', '%Y-%m-%d'),
        last_login = datetime.strptime('2024-3-11 10:14:54', '%Y-%m-%d %H:%M:%S')
    )
]

schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('id', LongType(), False),
    StructField('salary', DoubleType(), True),
    StructField('bonus', FloatType(), True),
    StructField('is_active', BooleanType(), True),
    StructField('scores', ArrayType(IntegerType(), True), True),
    StructField('attributes', MapType(StringType(), StringType()), True),
    StructField('hire_date', DateType(), True),
    StructField('last_login', TimestampType(), True)
])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()


df1 =  spark.range(1, 10).toDF('nums')
df1.show()