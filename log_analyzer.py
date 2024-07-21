import logging
import pandas as pd
from pyspark.sql.functions import col, desc, avg, explode, split, length, lower
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
from spark_utils import get_spark_session

logger = logging.getLogger(__name__)


def analyze_logs(log_df):
    logger.info(f"Analyzing logs DataFrame with {len(log_df)} rows")
    try:
        spark = get_spark_session()
        logs_df = spark.createDataFrame(log_df)
        logger.info(f"Created Spark DataFrame with schema: {logs_df.schema}")

        # Basic template analysis
        template_counts = logs_df.groupBy('log_template').count().orderBy(desc('count'))
        logger.info(f"Completed template analysis. Found {template_counts.count()} unique templates.")

        # Error analysis
        error_templates = logs_df.filter(col('log_template').like('%error%') | col('log_template').like('%failed%')) \
            .groupBy('log_template').count().orderBy(desc('count'))
        logger.info(f"Completed error analysis. Found {error_templates.count()} error templates.")

        # Cluster analysis
        cluster_counts = logs_df.groupBy('cluster_id', 'log_template').count().orderBy(desc('count'))
        logger.info(f"Completed cluster analysis. Found {cluster_counts.count()} clusters.")

        # Word frequency analysis
        word_counts = logs_df.select(explode(split(lower(col('log_message')), ' ')).alias('word')) \
            .groupBy('word').count().orderBy(desc('count'))
        logger.info(f"Completed word frequency analysis. Found {word_counts.count()} unique words.")

        # Log length analysis
        logs_df = logs_df.withColumn('log_length', length(col('log_message')))
        avg_log_length = logs_df.agg(avg('log_length').alias('avg_log_length'))
        logger.info(
            f"Completed log length analysis. Average log length: {avg_log_length.collect()[0]['avg_log_length']:.2f}")

        # Advanced NLP: Topic Modeling using LDA
        tokenizer = Tokenizer(inputCol="log_message", outputCol="words")
        wordsData = tokenizer.transform(logs_df)

        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        wordsData = remover.transform(wordsData)

        countVectorizer = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)
        cvmodel = countVectorizer.fit(wordsData)
        vectorized_data = cvmodel.transform(wordsData)

        num_topics = 5
        lda = LDA(k=num_topics, maxIter=10)
        model = lda.fit(vectorized_data)

        topics = model.describeTopics(maxTermsPerTopic=5)
        vocab = cvmodel.vocabulary
        topics_rdd = topics.rdd.map(lambda row: [vocab[idx] for idx in row['termIndices']])
        topics_terms = topics_rdd.collect()
        logger.info(f"Completed topic modeling. Extracted {num_topics} topics.")

        template_pairs = logs_df.alias('a').join(logs_df.alias('b'), on='cluster_id') \
            .where(col('a.log_template') < col('b.log_template')) \
            .groupBy('a.log_template', 'b.log_template').count() \
            .select(
            col('a.log_template').alias('template_1'),
            col('b.log_template').alias('template_2'),
            col('count')
        ) \
            .orderBy(desc('count')).limit(20)
        logger.info("Completed co-occurring template analysis.")

        logger.info("Log analysis completed successfully")

        return {
            "template_counts": template_counts.toPandas(),
            "error_templates": error_templates.toPandas(),
            "cluster_counts": cluster_counts.toPandas(),
            "word_counts": word_counts.toPandas(),
            "avg_log_length": avg_log_length.toPandas().iloc[0, 0],
            "topics": topics_terms,
            "template_pairs": template_pairs.toPandas()
        }
    except Exception as e:
        logger.error(f"Error during log analysis: {str(e)}", exc_info=True)
        raise
