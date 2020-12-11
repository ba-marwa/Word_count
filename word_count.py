from pyspark import SparkContext, SparkConf
 
#Instancier l'API de spark pour les RDD (spark context)

conf =SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)

#Importer le fichier

Fichier=sc.textFile("C:/word_count/input.txt")

#Réalisation  du word count

words = Fichier.flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

print(wordCounts.collect())
	
#Export du résultat
wordCounts.saveAsTextFile("C:/word_count/output")
