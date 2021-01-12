"""
				Module: NaiveBayes.py		Project: 1				Course: Data Science Practicum
				Author: Narinder Singh		Date: 08-Feb-2018		Semester: Spring'19

A Sparky implementation of the Naive Bayes classifier with Big Data capabilities.
"""


import os
import sys
import json

from pyspark.context import SparkContext
from argparse import ArgumentParser
from operator import add
from math import log
from zipfile import PyZipFile

from Transformations import *
import Utilities

class NaiveBayes:
	"""
		Sparky implementation of a Naive Bayes document classifier.
	"""

	def __init__(self, src, labels):
		"""
			Initialize the object with data source @src <string> path containing the documents and the @labels <string> file containing the labels against the document names (without the extension). @labels must be formatted as a key-value CSV with or without any headers.
		"""
		sc = SparkContext.getOrCreate()

		# Set data source and class labels
		self.src = src
		self.labels = labels
		
		# Default smoothing parameters - No smoothing
		self.alpha = 0.0
		self.vsize = 1.0
		
		# Set default word filter - No filter
		self.filter = lambda word: True
		
		# Compute prior probability of classes
		self.priors = self.computePriors(self.labels.values(), logs=False)
		
		# Make room for the conditional probabilities that will be computed.
		self.conditionals = None
	
		# No of data points the classifier has learned from.
		self.lsize = 0


	def getLabels(self):
		"""
			Returns the {document: label} dictionary.
		"""
		return self.labels

	def computePriors(self, labels, logs = True):
		"""
			Computes prior probabilities for labels.
		"""
		
		# Initialize a frequency table and count labels
		table = {}
		for label in labels:
			if label not in table: table[label] = 1.0
			else: table[label] += 1.0

		# Case 1: logs is set
		if logs:
			return {label: log(count/len(labels)) for label, count in table.items()}

		# Case 2: logs is not set
		else:
			return {label: count/len(labels) for label, count in table.items()}
		

	def setSmoothingParameters(self, alpha, vsize):
		"""
			Set the smoothing parameters for the multinomial model.
		"""
		self.alpha = alpha
		self.vsize = vsize
	
	def setN(self, n):
		"""
			Set 'n' in n-grams
		"""
		self.n = n
	
	def setWordFilter(self, filter):
		"""
			Set a new word filter to apply before computing ngrams.
		"""
		self.filter = filter
	
	def learn(self):
		"""
			Learning method. @n is the n in n-grams.
		"""
		sc = SparkContext.getOrCreate()
	
		# Read the documents as ngrams
		ngrams = computeNGramsRDD(self.src, n=self.n, filter=self.filter)
		
		# Record the number of data points that the classifier is learning from.
		self.lsize = ngrams.count()
		
		# Replace the documents names with class labels
		ngrams = swapKeysRDD(ngrams, self.labels)
	
		# Compute Term Frequencies
		tfs = computeTFsRDD(ngrams)
		
		# Compute IDFs
		idfs = computeIDFsRDD(ngrams, self.lsize)
		
		# Weigh TFs with IDFs
		tfidfs = weighTFsRDD(tfs, idfs.collectAsMap())
		
		# Compute log likelihoods
		self.conditionals = computeLikelihoodsRDD(tfidfs, alpha = self.alpha, vsize = self.vsize).collectAsMap()
	
	
	def predict(self, src, outfile = None):
		"""
			Predicting on a new data set.
		"""
		sc = SparkContext.getOrCreate()

		# Make n-grams. Resultant RDD format: (document <any>, ngram <tuple>)
		ngrams = computeNGramsRDD(src, n=self.n, filter=self.filter)
		
		# Broadcast the set of class labels.
		blabels = sc.broadcast(set(self.conditionals.keys()))
		
		# Prepare for estimating class probabilities against each document. Resultant RDD format: ((document, cls), ngram)
		ngrams = ngrams.flatMap(lambda (document, ngram): [((document, cls), ngram) for cls in blabels.value])

		# Broadcast the conditional probabilities.
		bmodel = sc.broadcast(self.conditionals)

		# Replace n-gram with the likelihood estimate. Resultant RDD format: ((document, cls), likelihood)
		likelihoods = ngrams.map(lambda ((document, cls), ngram): ((document, cls), bmodel.value[cls].get(ngram) or bmodel.value[cls].get("unseen")))

		# Sum up likelihoods. Resultant RDD format: ((document, cls), likelihood)
		likelihoods = likelihoods.reduceByKey(add)
		
		# Restructure the key value to make document the key for grouping. Resultant RDD format: (document, (class, likelihood))
		likelihoods = likelihoods.map(lambda ((document, cls), likelihood): (document, (cls, likelihood)))
		
		# Broadcast prior probabilities
		bpriors = sc.broadcast(self.priors)
		
		# Add prior probabilities to estimates. Resultant RDD format: (document, (class, likelihood))
		likelihoods = likelihoods.map(lambda (document, (cls, likelihood)): (document, (cls, likelihood + bpriors.value[cls])))
		
		# Now group predicitons by document. (document <any>, (class, likelihood) <iterator>)
		likelihoods = likelihoods.groupByKey()
		
		# Pick the best. Resultant RDD format: (document, (class, likelihood))
		likelihoods = likelihoods.map(lambda (document, estimates): (document, max(estimates, key = lambda (cls, lkl): lkl)))
				
		# Retain class. Drop likelihood estimates, only retain the class.
		predictions = likelihoods.map(lambda (document, (cls, lkl)): (document, cls))
		
		# Save predicitons to outfile, if provided
		if outfile is not None:
			with open(outfile, 'w') as outfile:
				json.dump(predictions.collectAsMap(), outfile)
		
		return predictions





