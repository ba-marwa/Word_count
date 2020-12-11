from pyspark import SparkContext, SparkConf
 
conf =SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)

	
Fichier=sc.textFile("C:/Users/sakur/OneDrive/Documents/Cours/Outils Data Mining/Test word count/sample.txt")

# read data from text file and split each line into words
words = Fichier.flatMap(lambda line: line.split(" "))
	
# count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

print(wordCounts.collect())
	
# save the counts to output
wordCounts.saveAsTextFile("C:/Users/sakur/OneDrive/Documents/Cours/Outils Data Mining/Test word count/")
