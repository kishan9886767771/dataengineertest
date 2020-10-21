## @package HelloFresh
#  HelloFresh is a Package to Read json and Calculate average cooking time duration per difficulty level using pyspark.
#
#  More details.

## INCLUDE MODULES
import json
import requests
import re
import pyspark
import logging
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import substring, expr, lower
logging.basicConfig(filename="hellofresh.log", format='%(asctime)s %(message)s', filemode='w', level=logging.INFO)

#Setting the threshold of logger
logging.getLogger('py4j').setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.ERROR)

#Creating an object
sc = SparkContext(master="local[4]", appName="HelloFresh").getOrCreate()
sqlContext = SQLContext(sc)
logging.info("Spark Context Created with Application Name : HelloFresh")

## PUBLIC VARIABLE DECLARATIONS
totalTime = {
    "easy" : { "time" : 0, "count": 0 },
    "medium" : { "time" : 0, "count": 0 },
    "hard" : { "time" : 0, "count": 0 }
}

## HelloFresh is a class to Read json and Calculate average cooking time duration per difficulty level using pyspark.
#
#  More details.
class HelloFresh():
    ## Constructor for HelloFresh Objects
    #  This constructor also fetch data from given URL and construct spark dataframe.
    # 
    #  More details.
    def __init__(self, url):
        try:
            result = requests.get(url)
            logging.info("S3 request is successful")            
        except:
            logging.error("Exception occurred {} is not a valid URL".format(url), exc_info=True)
            raise Exception("{} is not a valid URL".format(url))            
        if result.headers['content-type'] != "application/json":
            logging.error("Exception occurred {} is not a json file".format(url), exc_info=True)
            raise Exception("Response for {} is not a json file".format(url))
        self.df = sqlContext.createDataFrame([json.loads(line) for line in result.iter_lines()])
        self.easyAvgTime = 0
        self.mediumAvgTime = 0
        self.hardAvgTime = 0
        self.easyTime = 0
        self.hardTime = 0

    ## This method is used to convert object to string
    #  Return 3 lines of string with Average cooking time with Difficulty.
    #  
    #  More details.
    def __str__(self):
        return "\nDifficuly Level : Avg Cooking Time\nEasy Level : {}\nMedium Level : {}\nHard Level : {}\n".format(self.__convertToStr(self.easyAvgTime), self.__convertToStr(self.mediumAvgTime), self.__convertToStr(self.hardAvgTime))

    ## __convertToStr is used to convert minutes to Human Readable H M S string.
    #  @param self The object pointer \n
    #  @param minutes: Integer or Float value in minutes (EX: 13.5)
    #  @return: human readable string (EX: 00H 13M 05S)
    #  
    #  More details.
    def __convertToStr(self, minutes):
        seconds = minutes * 60
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return "%dH %02dM %02dS" % (hour, minutes, seconds)

    ## __convertToMin is used to convert Hour Minute string to minutes
    #  @param self The object pointer.
    #  @param time: String Input (EX: 2H15M)
    #  @return: integer value of minutes for given string (EX: 135)
    def __convertToMin(self, time):
        if not time:
            return 0
        time = time.upper()
        time = re.split('H|M', time)[:-1]
        if len(time) == 1:
            return int(time[0])
        elif len(time) == 2:
            return (int(time[0]) * 60) + int(time[1])

    ## __getTotalTime is used to get total of cookTime and prepTime
    #  @param item: json object with keys cookTime and prepTime
    #  @return: integer value for sum of cookTime and prepTime
    def __getTotalTime(self, item):
        item = json.loads(item)
        return self.__convertToMin(item['cookTime']) + self.__convertToMin(item['prepTime'])

    ## config is used to set the upperbound of easy difficulty and lowerbound of hard difficulty
    #  medium difficulty will be considered as between easy and hard
    #
    #  @param easy: integer value as a higher bound for easy difficulty
    #  @param hard: integer value as a lower bound for hard difficulty
    #  @return: True if configures successfully else False
    def config(self, easy, hard):
        logging.info("Control entered inside the config() method in HelloFresh class")
        if (easy < 0 and hard < 0):
            logging.error("Config values are less than 0")
            return False
        self.easyTime = easy
        self.hardTime = hard
        return True

    ## get Method is used to extract and compute Average time for all difficulty levels.
    #  function takes one potential argument, which will be used as keyword while searching in ingredients
    #  then extract cookTime and prepTime using pyspark.
    #
    #  Extracted values will be converted to minutes, compared with configured limits and compute average.
    # 
    #  @param keyword: string argument (will be used search items in json)
    #  @return: False if keyword is None else True
    def get(self, keyword):
        logging.info("Control entered inside the get() method in HelloFresh class")
        if not keyword:
            logging.error("get() method requires keyword in HelloFresh class")
            return False
        self.df = self.df.where(lower(self.df['ingredients']).like("%{}%".format(keyword))).select('cookTime','prepTime').withColumn("cookTime",expr("substring(cookTime, 3)")).withColumn("prepTime",expr("substring(prepTime, 3)"))
        for item in self.df.toJSON().collect():
            itemTotalTime = self.__getTotalTime(item)
            if itemTotalTime < self.easyTime:
                totalTime['easy']['time'] += itemTotalTime
                totalTime['easy']['count'] += 1
            elif itemTotalTime > self.hardTime:
                totalTime['medium']['time'] += itemTotalTime
                totalTime['medium']['count'] += 1
            else:
                totalTime['hard']['time'] += itemTotalTime
                totalTime['hard']['count'] += 1
        self.easyAvgTime = totalTime['easy']['time'] / totalTime['easy']['count']
        self.mediumAvgTime = totalTime['medium']['time'] / totalTime['medium']['count']
        self.hardAvgTime = totalTime['hard']['time'] / totalTime['hard']['count']
        return True

    ## save method is used to write extracted and computed data to given file
    #
    #  @param fileName: string Absolute / Relative FilePath with extention
    #  @return: False if failed to write file else True
    def save(self, fileName):
        logging.info("Control entered inside the save() method in HelloFresh class")
        if not os.path.exists(os.path.dirname(fileName)):
            os.makedirs(os.path.dirname(fileName))
        with open(fileName, 'w') as fp:
            fp.write("difficulty,avg_total_cooking_time\n")
            fp.write("easy,{}\n".format(self.__convertToStr(self.easyAvgTime)))
            fp.write("medium,{}\n".format(self.__convertToStr(self.mediumAvgTime)))
            fp.write("hard,{}\n".format(self.__convertToStr(self.hardAvgTime)))
            fp.close()            
            return True
        logging.error("Failed to writing the report file")
        return False