import time
import findspark
findspark.init()

import os
import pandas as pd

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import HashingTF, IDF, StringIndexer, SQLTransformer,IndexToString
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.ml import Pipeline

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0,com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

# retrieve data from the database
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df


def clustering(df, k, features_col="features", prediction_col="prediction"):
    start =time.time()
    # definition of a Kmeans model (clustering)
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol(features_col).setPredictionCol(prediction_col)
    # Trains a Kmeans model.
    model = kmeans.fit(df)

    # Make predictions
    predictions = model.transform(df)
    end=time.time()
    print("kmeans evaluation with k equal to {} in {} seconds".format(k,(end-start)))
    # Evaluate clustering by computing Silhouette score
    # If this number is negative, the data cannot be separated at all.
    # Values closer to 1 indicate maximum separation.
    # Values close to zero mean the data could barely be separated.
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # computeCost: return the Bisecting K-means cost (sum of squared distances of points to their nearest center) 
    # for this model on the given data (df)
    cost = model.computeCost(df)
    print("Within Set Sum of Squared Errors = " + str(cost))
    
    return predictions, silhouette#, model, cost

# converts a list of list into a single list
def flat_list(l):
    return  [item for sublist in l for item in sublist]

# compute wordcloud
def word_cloud(flat_sentences, k):
        counter = Counter(flat_sentences)
        cdict = dict(counter.most_common(50))
        wordcloud = WordCloud(background_color="white").generate_from_frequencies(cdict)
        plt.figure(figsize = (10, 8), facecolor = None) 
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.savefig("plot/cluster_"+str(k)+".png", format="png")

if __name__ == "__main__":

    # Create a local SparkContext with as many worker threads as logical cores on your machine
    sc = SparkContext(master = "local[*]", appName="ML")
    sc.setLogLevel('ERROR')
    sqlContext = SQLContext(sc)

    tweet = load_and_get_table_df("twitter", "tweet").select("id", "text").where("lang == \"en\"") # extracts and selects tweets in English language (id and text)
    print("number of tweets: ",tweet.count())

    # Each annotator in Spark NLP accepts certain types of columns and outputs new columns in another type (we call this AnnotatorType). 
    # To get through the process in Spark NLP, we need to get raw data transformed into Document type at first. 
    # DocumentAssembler() is a special transformer that does this for us; it creates the first annotation of type Document which may be
    # used by annotators.
    document_assembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    # convert document to array of tokens
    tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

    # Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. 
    # This normalization can help standardize your input data and improve the behavior of learning algorithms.
    normalizer = Normalizer() \
        .setInputCols(["token"]) \
        .setOutputCol("normalized") \
        .setLowercase(True)

    # remove stopwords
    stopwords_list = ["rt", "+", "us", "im"] 
    stopwords_list.extend(StopWordsCleaner().getStopWords())
    stopwords_list = list(set(stopwords_list))
    stopwords_cleaner = StopWordsCleaner()\
        .setInputCols("normalized")\
        .setOutputCol("cleanTokens")\
        .setCaseSensitive(False)\
        .setStopWords(stopwords_list)

    # stems tokens to bring it to root form
    stemmer = Stemmer() \
        .setInputCols(["cleanTokens"]) \
        .setOutputCol("stem")

    # Convert custom document structure to array of tokens.
    finisher_stopwords = Finisher() \
        .setInputCols(["cleanTokens"]) \
        .setOutputCols(["token_after_stopwords_cleaner"]) \
        .setOutputAsArray(True) \
        .setCleanAnnotations(False)
        
    finisher_stem = Finisher() \
        .setInputCols(["stem"]) \
        .setOutputCols(["token_features"]) \
        .setOutputAsArray(True) \
        .setCleanAnnotations(False)

    # TF -IDF
    # Term frequency-inverse document frequency (TF-IDF) is a feature vectorization method widely used in text mining to reflect the importance 
    # of a term to a document in the corpus.
    hashingTF = HashingTF(inputCol="token_features", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)

    # define the nlp pipeline
    nlp_pipeline = Pipeline(
        stages=[document_assembler, 
                tokenizer,
                normalizer,
                stopwords_cleaner, 
                stemmer,
                finisher_stopwords, 
                finisher_stem,
                hashingTF,
                idf])
    pipeline_start = time.time()

    # pipeline execution
    nlp = nlp_pipeline.fit(tweet)
    df = nlp.transform(tweet)
    
    pipeline_end = time.time()
    print("duration of nlp_pipeline in seconds:", (pipeline_end - pipeline_start))
    
    start_kmeans = time.time()

    print("start kmeans")

    # search for the best separation
    best_score = 0
    best_predictions = None
    best_k = 0
    for k in range(2,11):
        
        predictions, score = clustering(df, k)
        if score > best_score:
            best_score = score
            best_predictions = predictions
            best_k = k

    end_kmeans = time.time()
    print("best kmeans in {} seconds".format(end_kmeans - start_kmeans))
    clean_sentence = pd.DataFrame()

    print("best k: ", best_k)
    print("best score: ", best_score)
    #best_predictions.show()

    # converting pyspark dataframes to pandas dataframes to generate wordclouds
    clean_sentence= pd.concat([df.select("token_after_stopwords_cleaner").toPandas(),
        best_predictions.select("prediction").toPandas()],axis=1)

    # wordcloud creation
    for i in range(0, best_k):
        print("word_cloud {} done!".format(i))
        word_cloud(flat_list(clean_sentence.loc[clean_sentence["prediction"] == i]["token_after_stopwords_cleaner"]), i)

    sc.stop()