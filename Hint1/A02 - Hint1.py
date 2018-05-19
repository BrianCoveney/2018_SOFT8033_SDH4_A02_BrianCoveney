# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------
import json


# ------------------------------------------
# FUNCTION printRDD
# ------------------------------------------
def print_rdd(my_rdd):
    for item in my_rdd.take(4):
        print(item)


# FUNCTION get_avg
# ------------------------------------------
def get_avg(val1, val2):
    res = float(float(val1) / float(val2))
    return res


# ------------------------------------------
# FUNCTION process_items(x)
# ------------------------------------------
def process_items(x):
    cuisine, evaluation, points = x["cuisine"], x["evaluation"], x["points"]
    return cuisine, (points, evaluation)


# ------------------------------------------
# FUNCTION aggregate_info(x)
# Point 1. Aggregate info per type of cuisine
#          Example: (u'Donuts', (16, 0, 137))
# ------------------------------------------
def aggregate_info(x):
    cuisine, reviews = x[0], x[1]
    total_reviews, total_neg_reviews, total_points = 0, 0, 0

    for r in reviews:
        review_tuple = r[1]
        evaluation, points = review_tuple[1], review_tuple[0]

        if evaluation == "Negative":
            total_neg_reviews += 1
            total_points -= points
        else:
            total_points += points

        total_reviews += 1

    return cuisine, (total_reviews, total_neg_reviews, total_points)


# ------------------------------------------
# FUNCTION remove_info
# Remove entries that do not satisfy conditions
# ------------------------------------------
def remove_info(x, avgReviews, percentage_f):
    total_reviews, total_neg_reviews = x[1][0], x[1][1]
    neg_reviews_percentage = get_avg(total_neg_reviews, total_reviews) * 100

    if total_reviews >= avgReviews and neg_reviews_percentage < percentage_f:
        return True


# ------------------------------------------
# FUNCTION aggregate_info(x)
#  Aggregate info per type of cuisine, incl avg points
#  (u'Hamburgers', (1676, 107, 11190, 6.676610978520286))
# ------------------------------------------
def aggregate_avg_points(x):
    cuisine, total_reviews, total_neg_reviews, points = x[0], x[1][0], x[1][1], x[1][2]
    avg_points_per_review = get_avg(points, total_reviews)

    return cuisine, (total_reviews, total_neg_reviews, points, avg_points_per_review)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
    # Point 1  ------------------------------------------
    #
    inputRDD = sc.textFile(dataset_dir)
    mapRDD = inputRDD.map(lambda x: json.loads(x))
    processItemsRDD = mapRDD.map(lambda x: process_items(x))
    groupByKeyRDD = processItemsRDD.groupBy(lambda x: x[0])

    aggRDD = groupByKeyRDD.map(lambda x: aggregate_info(x))
    # print_rdd(aggRDD)

    # Point 2 ------------------------------------------
    #
    avgReviews = get_avg(mapRDD.count(), aggRDD.count())
    # print(avgReviews)

    # Point 3 ------------------------------------------
    #
    removedEntriesRDD = aggRDD.filter(lambda x: remove_info(x, avgReviews, percentage_f))
    # print_rdd(removedEntriesRDD)

    # Point 4 ------------------------------------------
    #
    aggAvgPointsRDD = removedEntriesRDD.map(lambda x: aggregate_avg_points(x))
    solutionRDD = aggAvgPointsRDD.sortBy(lambda x: x[1][3], ascending=False)
    print_rdd(solutionRDD)

    solutionRDD.saveAsTextFile(result_dir)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, makin th Pytho interprete to trigge
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    percentage_f = 10

    dbutils.fs.rm(result_dir, True)

    my_main(source_dir, result_dir, percentage_f)
