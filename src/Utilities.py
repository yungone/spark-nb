"""
				Module: Utilities.py		Project: 1				Course: Data Science Practicum
				Author: Narinder Singh		Date: 03-Feb-2018		Semester: Spring'19
					
The module contains methods and classes to make life easier.
"""

import os
import sys

class UtilitiesError(Exception): pass
class BadInputError(UtilitiesError): pass


from pyspark.context import SparkContext


class ProgressBar:
	"""
		A handrolled implementation of a progress bar. The bar displays the progress as a ratio like this: (1/360).
	"""

	def __init__(self, max = 100):
		"""
			Initialize the bar with the total number of units (scale).
		"""
		self.max = max
		self.current = 0
		print "Initiating download..... \n"

	def update(self, add = 1):
		"""
			Record progress.
		"""
		self.current += add
		self._clear()
		self._display()

	def _display(self):
		"""
			Print the completion ratio on the screen.
		"""
		print "(" + str(self.current) + "/" + str(self.max) + ")"

	def _clear(self):
		"""
			Erase the old ratio from the console.
		"""
		sys.stdout.write("\033[F")
		sys.stdout.flush()


def stripExtension(filename):
	"""
		Strips the file extension at the end of the @filename <string>, if present. The implementation simply looks for the last period in the filename and if found, deletes everything thereafter.
		
		returns: <string> the stripped filename
	"""
	# Empty string
	if not filename: return filename
	
	# Strip any whitespace
	filename = filename.strip()
	
	# Search for the period starting from the end.
	index = len(filename) -1
	while filename[index] != '.' and index > -1: index -= 1

	# Case 1. No period found
	if index == -1: return filename

	# Case 2. Period found
	return filename[:index]
	

def flen(filename):
	"""
		File LENgth computes and returns the number of lines in a file. @filename <string> is path to a file. This is an epensive method to call for the whole file is read to determine the number of lines.
		
		returns: <integer> line count
	"""
	# Read and count lines.
	with open(filename, 'r') as infile:
		return sum((1 for line in infile))


def dlen(dirname):
		"""
			This horribly named method computes the no. of (non-hidden) entries in the directory @dirname <string>.
			
			returns: <integer> line count
		"""
		contents = os.listdir(dirname)
		contents = [element for element in contents if not element.startswith(".")]
		return len(contents)
		

def readKeyValueCSV(filename):
	"""
		Reads a key-value csv file and returns the contents as a python dictionary. @filename is the path to the file.
		
		returns: file contents as <dictionary>
	"""
	# Resultant dictionary.
	result = {}
	
	# Read file and collect key-value pairs into result.
	with open(filename, "r") as infile:
		for line in infile:
			key, value = line.split(",")
			result[key.strip()] = value.strip()

	return result
	

def merge(first, second, outfile):
	"""
		The procedure merges the two text files line by line, separated by commas, into a new file. For instance, the files - A.txt and B.txt are merged to generate C.txt. All the arguments are paths to files passed as <string>s and the files to be merged must have equal number of lines.
		
			A.txt	|		B.txt	|		C.txt
		A			|	1			|	A, 1
		B			|	2			|	B, 2
		C			|	3			|	C, 3
					|				|
					
		returns: <True> on completion.
	"""
	# Make sure the files have equal number of lines.
	if not flen(first) == flen(second): raise BadInputError("The files: " + first + " and " + second + " must have equal number of lines.")

	# Read and merge.
	with open(first, 'r') as first, open(second, 'r') as second, open (outfile, 'w') as outfile:
		while True:
			try:
				# Read lines.
				a = first.next()
				b = second.next()
				# Write as: a, b
				outfile.write(a.strip() + ', ' + b.strip() + '\n')
			except StopIteration:
				break

	return True


def matchDictionaries(first, second):
	"""
		Match key value pairs in the first dictionary with the second and return the number of matches. Absence of a key in the second dictionary is counted as a mismatch and extraneous keys in the second dictionary are ignored.
	"""
	matches = 0
	for key in first:
		if key not in second: continue
		if first[key] == second[key]: matches += 1

	return matches


def readFilenames(file, path = "", extension = ""):
	"""
		Read the filenames from @file <string>. @file is a text file containing names of files, one per line. Optionally, an @extension <string> parameter can be passed which will be appended to each filename and similarly, the optional @path <string> parameter can be passed which will be prepended to each filename.
	"""
	# Read each name from file and collect in results
	results = []
	with open(file, 'r') as infile:
		for name in infile:
			results.append(os.path.join(path, name.strip() + extension))

	return results


def readLines(file, map=lambda line: line.strip()):
	"""
		Read and return a file as a list of files. Optionally, @map argument can be set to process the lines.
	"""
	# Read file line by line.
	results = []
	with open(file, 'r') as infile:
		for line in infile:
			results.append(map(line))

	return results


def mapLists(first, second):
	"""
		Make a dictionary from two lists with elements of the first as the keys and second as values. If there are more elements in the first list, they are assigned None values and if there are more in the second list, they're dropped.
	"""
	index = 0
	dict = {}
	# Read through every index of the @first list and build the map.
	while index < len(first):
		f = first[index]
		s = second[index] if index < len(second) else None
		dict[f] = s
		
		index += 1

	return dict
	

if __name__ == '__main__':
	pass

