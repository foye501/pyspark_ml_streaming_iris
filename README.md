# pyspark_ml_streaming_iris
Example of combining spark.ml and structured-streaming

Model training is a batch process.
However inference (model.transform) - is done in real-time.
In spark.ml model.transform(df) can be used on ANY df - that is
on a DataFrame which is bounded and also on a DataFrame which is unbounded.

In this example we take the iris.csv data set, train a regression model over it
and then take data , stream it through Kafka and do the prediction over that stream.

Files included:
* iris.csv - the data set.
* iris2kafka.py - stream random lines from the data indo a Kafka broker.
* iris_streaming.py - a pyspark application that does: 1. model training; 2. inference over streamed data
