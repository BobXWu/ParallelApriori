# -*- coding: UTF-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession
import IPython


sc = SparkContext('local', 'pyspark')
spark = SparkSession \
    .builder \
    .appName("Test Data Analysis") \
    .getOrCreate()


def apriori(rdd, max_k=100, min_support=.5):
    def count_step(x):
        support = 0
        for var in D_bc.value:
            if x.issubset(var):
                support += 1
        support /= quantity
        if support >= min_support:
            return (x, round(support, 2))

    def generate_frequent(C_rdd, min_support, freq_set, last_support_L_rdd=None):
        support_L_rdd = C_rdd.map(count_step).filter(lambda x: x)
        support_L_rdd.persist()
        freq_set += [(list(x[0]), x[1]) for x in support_L_rdd.collect()]
        if last_support_L_rdd:
            last_support_L_rdd.unpersist()
        L_rdd = support_L_rdd.map(lambda x: x[0])
        return L_rdd, support_L_rdd

    def connect(x):
        ret = []
        output = x[1]
        for i, a in enumerate(output):
            for b in output[i+1:]:
                ret.append(x[0] + (min(a, b), max(a, b)))
        return ret

    def split(x, k):
        t = tuple(x)
        return (t[:k-1], [ t[k-1] ])

    def pruning(x, k, L):
        l = list(x)[:len(x)-2]
        has_infrequent = False
        for element in l:
            x.remove(element)
            flag = False
            for freqset in L:
                if x == freqset:
                    flag = True
            x.add(element)
            if not flag:
                has_infrequent = True
                break
        return has_infrequent

    def generate_candidate(L_rdd, k):
        L = L_rdd.collect()
        C_rdd = L_rdd.map(lambda x: split(x, k)).reduceByKey(
            lambda x, y: x + y).flatMap(connect).map(lambda x: frozenset(x))#.filter(lambda x: not pruning(x, k, L))
        return C_rdd

    if not rdd:
        rdd = [[1, 4, 3, 5], [2, 3, 5], [1, 2, 3, 4, 5], [2, 3, 4, 5]]
        rdd = sc.parallelize(rdd)

    C1_rdd = rdd.flatMap(
        lambda x: set(x))\
        .distinct()\
        .map(lambda x: frozenset([x]))

    D = rdd.map(lambda x: set(x)).collect()
    D_bc = sc.broadcast(D)
    quantity = float(len(rdd.collect()))

    freq_set = []
    L1_rdd, support_L_rdd = generate_frequent(C1_rdd, min_support, freq_set)
    L2_rdd = None
    k = 2
    old_freq_set_length = 0
    new_freq_set_length = len(freq_set)
    while (new_freq_set_length - old_freq_set_length) and k <= max_k:
        C2_rdd = generate_candidate(L1_rdd, k-1)
        L2_rdd, support_L_rdd = generate_frequent(C2_rdd, min_support, freq_set, support_L_rdd)
        L1_rdd = L2_rdd
        k += 1
        old_freq_set_length = new_freq_set_length
        new_freq_set_length = len(freq_set)

    # support_L_rdd.unpersist()

    print "k: ", k - 1
    print "length of freqset: ", len(freq_set)
    print "min_support: ", min_support
    freq_set_df = spark.createDataFrame(freq_set, ['items', 'freq'])
    return freq_set_df


def tests_logs_analysis():
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
        "uri",
        "mongodb://localhost:28001/logs.tests2").load()
    rdd = df.select(['run', 'passed', 'testID']).rdd
    run_case_rdd = rdd.filter(lambda x: not x['passed']).map(
        lambda x: (x['run']['oid'], [x['testID']])).reduceByKey(
        lambda x, y: x + y).map(lambda x: list(set(x[1])))

    min_support = 0.2
    max_k = 100
    freqItemsets_df = apriori(run_case_rdd, max_k, min_support)
    result_df = spark.createDataFrame(
        [(
            freqItemsets_df.collect(),
        )],
        [
            'frequent_sets'
        ]
    )

    result_df.write.format('com.mongodb.spark.sql.DefaultSource').option(
    'uri', 'mongodb://127.0.0.1:28001/analysis.result_test').mode('append').save()

    return freqItemsets_df

# apriori(None)
tests_logs_analysis()
sc.stop()
