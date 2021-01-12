"""
				Module: Main.py				Project: 1				Course: Data Science Practicum
				Author: Narinder Singh		Date: 10-Feb-2018		Semester: Spring'19

The main script to make the model.
"""

from argparse import ArgumentParser
from pyspark.context import SparkContext


import Utilities
from NaiveBayes import NaiveBayes


def createLabelsMap(filenames, labels):
	"""
		Create a labels map with filename: label from the given @filenames and @labels files.
	"""
	filenames = Utilities.readLines(filenames, map=Utilities.stripExtension)
	labels = Utilities.readLines(labels)
	return Utilities.mapLists(filenames, labels)


def printAccuracies(predictions, expected):
	"""
		Print accuracies by comparing the predictions map with the expected map.
	"""
	matches = Utilities.matchDictionaries(predictions, expected)
	total = float(len(predictions))
	accuracy = matches/total

	print "Matches: ", matches
	print "Total: ", total
	print "Accuracy: ", accuracy


if __name__ == '__main__':
	# Parse command line arguments
	parser = ArgumentParser("Naive Bayes Document Classifier")

	parser.add_argument("-d", "--data", help = "HDFS Data source directory path containing documents to learn from.", required = True)
	parser.add_argument("-t", "--tdata", help = "HDFS Data Source directory path containing documents to test on.")

	parser.add_argument("-l", "--learn", help = "Path to file containing filenames to learn from.", required = True)
	parser.add_argument("-p", "--predict", help = "Path to file containing filenames to predict on.", required = True)

	parser.add_argument("-e", "--extension", help = "File extension for filenames.")
	parser.add_argument("-c", "--labels", help = "Path to file containing class labels as - filename, label - per line. ", required = True)

	parser.add_argument("-n", "--gramsize", help = "The n in n-gram.")
	parser.add_argument("-a", "--alpha", help = "The additive smoothing parameter.")
	
	parser.add_argument("-r", "--results", help = "Path to file containing expected labels for test files in the same order.")
	parser.add_argument("-o", "--outfile", help = "The outfile to write predictions to.")

	args = parser.parse_args()

	# Get spark context and set console output to be succinct
	sc = SparkContext.getOrCreate()
	sc.setLogLevel("ERROR")

	# Retrieve gram size and compute the vocabulary size from it
	n = int(args.gramsize or 1)
	vsize = 16**(2*n)

	# Alpha - The smoothing parameter
	alpha = float(args.alpha or 1.0)

	# Get source filenames for training and testing
	trainFiles = Utilities.readFilenames(args.learn, path=args.data, extension=args.extension or "")
	testFiles = Utilities.readFilenames(args.predict, path=args.tdata or args.data, extension=args.extension or "")

	# Create a labels map for training
	labelmap = createLabelsMap(args.learn, args.labels)
	
	# Create a new Naive Bayes documment classifier.
	# Note: The Naive Bayes classifier exposes the Hadoop input format and can take in a comma separated string of filenames.
	classifier = NaiveBayes(",".join(trainFiles), labelmap)

	# Set smoothing parameters and gram size
	classifier.setSmoothingParameters(alpha, vsize)
	classifier.setN(n)

	# Learn!
	classifier.learn()

	# Predict (and save predictions to outfile, if passed)
	# Note: The Naive Bayes classifier exposes the Hadoop input format and can take in a comma separated string of filenames.
	predictions = classifier.predict(",".join(testFiles), outfile=args.outfile).collectAsMap()

	# Compute accuracy from results, if provided
	if args.results:
		expected = createLabelsMap(args.predict, args.results)
		printAccuracies(predictions, expected)




