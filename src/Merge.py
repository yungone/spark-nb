"""
					Module: Merge.py			Project: 1				Course: Data Science Practicum
					Author: Narinder Singh		Date: 03-Feb-2018		Semester: Spring'19

This module makes a script out of the merge method fom the Utilities module. Makes life easier, that's all.
"""

import os
from argparse import ArgumentParser

import Utilities

if __name__ == '__main__':
	# Parse arguments.
	parser = ArgumentParser("Merge two text files with equal number of lines, line by line.")
	parser.add_argument("-f", "--first", help = "The first text file.", required = True)
	parser.add_argument("-s", "--second", help = "The second text file.", required = True)
	parser.add_argument("-o", "--outfile", help = "The output file.", required = True)

	args = parser.parse_args()
	Utilities.merge(args.first, args.second, args.outfile)

