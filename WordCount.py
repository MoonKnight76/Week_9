from pyspark import SparkContext
#importing stdin for checking DAG. This import will not terminate the code and the code will keep running until any KEY is pressed
#from sys import stdin
# if __name__ == "__main__":

sc = SparkContext("local[*]", "wordcount")
#Setting Logging level
sc.setLogLevel("ERROR")

    # Common Lines
    # input = sc.textFile("C:/Users/Rajat/OneDrive/Desktop/search_data.txt")
input = sc.textFile("C:/Users.Rajat/OneDrive/Desktop/TrendyTech/Week9/search_data.txt")
words = input.flatMap(lambda x: x.split(" "))
lowword=words.map(lambda x: (x.lower(),1))
finalcount = lowword.reduceByKey(lambda x, y: x+y)
rdd1=finalcount.map(lambda x: (x[1],x[0]))
rddfinal=rdd1.sortByKey(False)
rddresult=rddfinal.map(lambda x: (x[1],x[0]))
result = rddresult.collect()

for a in result:
    print(a)
# else:
#     print("Not executed directly")
#
# stdin.readline()

'''
Pyspark uses API library but SCALA was connecting to spark core directly hence Scala DAG Matches to our code but 
Pyspark code doesnt
'''



