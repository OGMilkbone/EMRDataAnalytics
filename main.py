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




if __name__ == "__main__":
    if len(argv) < 2:
        log("A file name is required as an argument at minimum")
        exit(-1)

    clear_resources()
    file_name = argv[1]
    split_file_name = file_name.split('.')
    extension = split_file_name[len(split_file_name) - 1]
    if extension != 'tsv':
        log("Warning: This program is designed to process tsv files, other file types are unsupported")

    log(str(number_of_records(file_name)))
    log(str(get_title(file_name)))