#
# pyspark 2.7 program to train on iris data set and do inference in streaming.
# Written by Tal Franji
# Code is free for all usage - feel free to copy and use.
import pyspark.ml
import pyspark.ml.feature
import pyspark.ml.classification
import pyspark.sql.functions as funcs
from pyspark.ml.linalg import Vectors, VectorUDT
import time


# based on https://github.com/Azure/MachineLearningSamples-Iris/blob/master/iris_spark.py

# Get the jar files from:
# http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.1.1/spark-sql-kafka-0-10_2.11-2.1.1.jar
# http://central.maven.org/maven2/org/apache/kafka/kafka-clients/0.10.2.1/kafka-clients-0.10.2.1.jar

# run this program using
#$ spark-submit --master="local[*]" --jars spark-sql-kafka-0-10_2.11-2.1.1.jar,kafka-clients-0.10.2.1.jar iris_streaming.py
def load_iris(spark):
    iris = spark.read.format("csv"
         ).option("header", "true"
         ).option("inferSchema", "true"
         ).load("iris.csv")
    iris.printSchema()
    return iris

def create_features_labels(iris):
    # vectorize all numerical columns into a single feature column
    feature_cols = iris.columns[:-1]  # all features besides "species" string column
    # assembler - turn 4 colmuns into a single column which is a vector of 4 floats.
    assembler = pyspark.ml.feature.VectorAssembler(inputCols=feature_cols, outputCol='features')
    # label_indexer - convert string label values into integer numerical values
    label_indexer = pyspark.ml.feature.StringIndexer(inputCol='species', outputCol='label')
    # pipe - a model that chains several models -
    pipe = pyspark.ml.Pipeline(stages=[assembler, label_indexer])
    pipe_model = pipe.fit(iris)
    data = pipe_model.transform(iris)
    return data.select("features", "label")


def train_model(data):
    # use Logistic Regression to train on the training set
    train, test = data.randomSplit([0.70, 0.30])
    lr = pyspark.ml.classification.LogisticRegression(regParam=0.01)
    model = lr.fit(train)
    return model


def get_feature_stream(spark):
    # You need to download Kafka jars when running this notebook.
    KAFKA = '34.242.73.97:9092'
    TOPIC = 'wwwlog'
    iris_raw_stream = spark.readStream.format("kafka")\
     .option("kafka.bootstrap.servers", KAFKA)\
     .option("subscribe", TOPIC)\
     .load()

    # convert Kafka's bin value to string
    raw1 = iris_raw_stream.selectExpr("CAST(value AS STRING) as val")
    # parse test line - split over ",". values is a list of strings
    raw2 = raw1.selectExpr("split(val, ',') as values", "split(val, ',')[4] as species")
    # take 4 first strings an create a single array of float named 'arr'
    features_array = raw2.selectExpr("""array(CAST(values[0] AS FLOAT), CAST(values[1] AS FLOAT), CAST(values[2] AS FLOAT), CAST(values[3] AS FLOAT)) as arr""", "species")
    # create a column 'features' which is a spark.ml vector instead of a spark.sql array
    tovec_udf = funcs.udf(lambda r : Vectors.dense(r),VectorUDT())
    data_stream = features_array.withColumn("features", tovec_udf("arr"))
    return data_stream


def show_30_seconds(streaming_df):
    # show a stream for 30 seconds and close query
    # this is useful to show some data on screen and stop program.
    q = streaming_df.writeStream.outputMode("append").format("console").start()
    time.sleep(30)
    q.stop()


def predict_on_stream(spark):
    iris = load_iris(spark)
    data = create_features_labels(iris)
    model = train_model(data)
    # Note - above is the training stage. model can now be saved and loaded by the streaming application.
    # From here starts the streaming part that uses the model.
    stream = get_feature_stream(spark)
    predictions = model.transform(stream)
    show_30_seconds(predictions)


def quiet_logs(spark):
    # hide some of the spark logging so output on screen may be useful.
    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def main():
    spark = pyspark.sql.SparkSession.builder.appName('iris_streaming').getOrCreate()
    quiet_logs(spark)
    predict_on_stream(spark)


if __name__ == "__main__":
    main()