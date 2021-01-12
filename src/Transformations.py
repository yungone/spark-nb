"""
				Module: Pipeline.py			Project: 1				Course: Data Science Practicum
				Author: Narinder Singh		Date: 04-Feb-2018		Semester: Spring'19

Data tranformation methods for documents built on top of Apache Spark.
"""

import os
import sys

from pyspark.context import SparkContext
from argparse import ArgumentParser
from operator import add
from math import log

import Utilities


def makeNGrams(string, n = 1, filter = lambda word: True):
	"""
		Takes in a @string <str> of words and generates and returns a tuple of n-grams. N-grams are computed after only keeping the words that pass the @filter <function> (words for which the @filter returns True are kept in the list). Default @n = 1 and default @filter passes everything.
		
		returns: <list> of all n-grams in @string, in order.
	"""
	# Split into words and filter.
	words = tuple((word for word in string.split() if filter(word)))
	
	# Pathological case with less words than n.
	if len(words) < n: return ()

	# Sliding window for extracting n-gram, exclusive on the upper bound.
	start, end = 0, n
	
	# Scan the word and collect n-grams.
	ngrams = []
	while end <= len(words):
		# extract
		ngrams.append(words[start: end])
		# slide
		start, end = start +1, end +1

	return ngrams


def swapKeysRDD(rdd, keymap):
	"""
		The method swaps the keys of a key-value RDD with a new set of keys provided in @keymap.
		
		returns: a new <pyspark.rdd.RDD>
	"""
	sc = SparkContext.getOrCreate()
	
	# Broadcast new keys
	keymap = sc.broadcast(keymap)

	# Swap old keys with new ones.
	rdd = rdd.map(lambda (key, value): (keymap.value[key], value))

	return rdd


def computeNGramsRDD(source, n = 1, filter = lambda x: True):
	"""
		Takes a data source directory @source <string> and reads all the text files from it, breaks down the contents of each text file into n-grams (using given @n), and makes a separate key-value pair with the source filename as key for each of the n-grams. The method returns an rdd with the following format: (filename <str>, n-gram <tuple>).
		
		Additional Notes: 1. Resultant filename would be the name of the text file minus the extension.
	"""
	# Read in the data
	sc = SparkContext.getOrCreate()
	
	# Resulant RDD elements' format: (filepath <str>, filecontents <str>).
	files = sc.wholeTextFiles(source)
	
	# Resultant RDD elements' format: (filename <str>, filecontents <str>).
	files = files.map(lambda (filename, string): (Utilities.stripExtension(os.path.basename(filename)), string))
	
	# Resultant RDD elements' format: (filename <str>, n-gram <tuple>).
	ngrams = files.flatMapValues(lambda string: makeNGrams(string, n, filter))

	return ngrams


def computeTFsRDD(occurances):
	"""
		Transforms an RDD of type (document <any>, word <any>) into a new one recording word frequencies against every document as a map. The resultant RDD has the format: (document <any>, {word <any> : count <any>}).

	"""

	# Prep for frequencies. Resultant RDD elements' format: ((document <any>, word <any>), 1)
	occurances = occurances.map(lambda pair: (pair, 1))
	
	# Count. Resultant RDD elements' format: ((document <any>, word <any>), count)
	counts = occurances.reduceByKey(add)
	
	# Shuffle key-value to make document the key. Resultant RDD elements' format: (document <any>, (word <any>, count <int>))
	counts = counts.map(lambda ((document, word), count): (document, (word, count)))

	# Group word counts by document. Resultant RDD elements' format: (document <any>, (word <any>, count <int>) <iterable>)
	counts = counts.groupByKey()
	
	# 'Flatten' the iterators. Resultant RDD elements' format: (document <any>, {word <any>: count <int>} <dictionary>)
	counts = counts.mapValues(lambda wcns: {word: count for word, count in wcns})

	return counts


def computeIDFsRDD(occurances, N):
	"""
		Input: RDD occurances: (document <any>, word <any>), N = Total no. of documents
		Output: RDD (word <any>, idf-score <double>)
	"""
	sc = SparkContext.getOrCreate()

	# Swap keys with values, do away with multiple identical occurances. Resultant RDD elements's format: (word <any>, document <any>)
	occurances = occurances.map(lambda (document, word): (word, document)).distinct()

	# Group the documents by word
	occurances = occurances.groupByKey()

	# Broadcast document count N to executors
	N = sc.broadcast(float(N))
	
	# Function to compute idf scores
	func = lambda docs: log(N.value/sum((1 for doc in docs)))

	# Compute idfs
	idfs = occurances.mapValues(func)
	
	return idfs


def weighTFsRDD(tfs, weights):
	"""
		Input tfs RDD: (document, {word: count})
			  weights: {word, weight}
		Output RDD: (document, {word: weight * count})
	"""
	sc = SparkContext.getOrCreate()

	# Broadcast weights to executors
	weights = sc.broadcast(weights)

	# Compute weighted term frequencies
	wtfs = tfs.mapValues(lambda (table): {word: table[word] * weights.value[word] for word in table})
	
	return wtfs


def computeLikelihoodsRDD(tfs, alpha = 0.0, vsize = 1.0, logs = True):
	"""
		This method takes in an RDD of multinomial distribution and computes likelihoods from the distributon. The flag @clogs dictates whether or not to take log of likelihoods. The mehod can perform additive smoothing before computing the estimator. @alpha <float> is the smoothing parameter and @vsize <int> (|V|) is the size of the vocabulary. Refer the wikipedia article for more on additive smoothing: https://en.wikipedia.org/wiki/Additive_smoothing. No smoothing is performed by default.
		
		Input RDD: (document <any>, {word: count})
		Output RDD: (document <any>, {word: likelihood, .... , "unseen": likelihood})
		Note: The likelihood of an unseen attribute computed via additive smoothing is added to the word-likelihood map with the key 'unseen' <string>.
	"""
	sc = SparkContext.getOrCreate()

	# Broadcast the variables required for computing likelihoods
	logs = sc.broadcast(float(logs))
	alpha = sc.broadcast(float(alpha))
	vsize = sc.broadcast(float(vsize))
	
	# Define map method for computing likelihoods from frequency table
	# Note that the method is defined within for it uses the variables broadcasted above and wouldn't make sense as a generic mehtod of the module.
	def computeLikelihoods(table):
		"""
			The map function for likelihhood estimation.
		"""
		# Total weight
		total = sum((table[word] for word in table))
		
		# Case 1. logs flag is set: Compute log likelihoods
		if logs.value:
			# log (tf + alpha / total + alpha * |V|)
			newtable = {word: log((table[word] + alpha.value) / (total + alpha.value * vsize.value)) for word in table}
			newtable["unseen"] = log(alpha.value / (total + alpha.value * vsize.value))

		# Case 2. Compute vanilla likelihoods
		else:
			newtable = {word: (table[word] + alpha.value) / (total + alpha.value * vsize.value) for word in table}
			newtable["unseen"] = alpha.value / (total + alpha.value * vsize.value)
	
		return newtable

	# Transform term frequencies RDD into an estimator
	estimator = tfs.mapValues(computeLikelihoods)

	return estimator
















