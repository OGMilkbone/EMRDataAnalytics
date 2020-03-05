from pyspark import SparkConf, SparkContext

from sys import argv

# common initialization for items used by the entire script

conf = (SparkConf()
        .setMaster("local")
        .setAppName("John EMR Test")
        )

sc = SparkContext(conf=conf)


def number_of_records(target_file):
    text_file_rdd = sc.textFile(target_file)
    return text_file_rdd.count()


def log(output):
    # logger function for controlled output
    # Don't use print, use this instead
    with open('logs/logfile.txt', 'a') as outfile:
        outfile.write(output)
        outfile.write('\n')


def clear_resources():
    # Clears the common resources used by the program
    # Just the log file for now
    with open('logs/logfile.txt', 'w+') as outfile:
        outfile.write('')

def get_title(target_file):
    text_file_rdd = sc.textFile(target_file)
    return text_file_rdd.first()

def review_analysis(target_file, term):
    text_file_rdd = sc.textFile(target_file)
    text_file_rdd = text_file_rdd.map(lambda x: x.split("\t"))
    #remove header
    text_file_rdd = text_file_rdd.filter(lambda x: x[0] != "marketplace")
    text_file_rdd = text_file_rdd.map(lambda x: [x[5], x[7], x[12]]) #just grabs the important stuff
    text_file_rdd = text_file_rdd.filter(lambda x: term in x[2])
    text_file_rdd = text_file_rdd.map(lambda x: [x[1], x[2]]).groupByKey().mapValues(list).collect()
    return text_file_rdd



if __name__ == "__main__":
    if len(argv) < 3:
        log("A file name and a search word is required as an argument at minimum")
        exit(-1)

    clear_resources()
    file_name = argv[1]
    search_term = argv[2]
    split_file_name = file_name.split('.')
    extension = split_file_name[len(split_file_name) - 1]
    if extension != 'tsv':
        log("Warning: This program is designed to process tsv files, other file types are unsupported")

    log(str(number_of_records(file_name)))
    log(str(get_title(file_name)))
    results = review_analysis(file_name, search_term)
    log(str(results))
    for key, value in results:
        log(str(key))
        log(str(value))