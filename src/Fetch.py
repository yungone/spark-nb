"""
					Module: Fetch.py			Project: 1				Course: Data Science Practicum
					Author: Narinder Singh		Date: 03-Feb-2018		Semester: Spring'19

Use this module to download files form web urls to your local machine. The module can be run as is like a script but is designed as a set of generic method(s) so that the code can be imported elsewhere as well. Running the script downloads malware file data form the uga-dsp Google Cloud bucket.
"""

import os
import sys
import urllib2

import Utilities

from argparse import ArgumentParser

class FetchError(Exception): pass
class BadFiletypeError(FetchError): pass


def fetchFiles(filenames, outdir, base_url, extension = ""):
	"""
		This method downloads files form web by generating urls using the given parameters. @filenames <string> is path to a text file that contains names of the files to be downloaded (one per line) and @base_url <string> is their location on the web. Optionally an @extension <string> parameter can be passed that will be appended to each filename. @outdir <string> is the path to the output directory where the downloads will be saved.
		
		returns: <True> on completion.
	"""
	# Bar to track progress of the download.
	bar = Utilities.ProgressBar(Utilities.flen(filenames))

	with open(filenames, "r") as names:
		for name in names:
			# Make file url.
			fname = name.strip() + extension
			furl = os.path.join(base_url, fname)

			# Make the http request and get the file.
			response = urllib2.urlopen(furl)
			code = response.read()

			# Save file.
			with open(os.path.join(outdir, fname), "w") as outfile:
				outfile.write(code)
			
			# Display progress.
			bar.update()


	return True


if __name__ == '__main__':
	# Parse arguments.
	parser = ArgumentParser("Fetch Malware files from Google Cloud")

	parser.add_argument("-n", "--filenames",
						help = "Input text file to read hashnames from. The file must be formatted as one hash per line.",
						required = True)
	parser.add_argument("-o", "--outdir",
						help = "Output file directory path to save the files into. Last level of the directory will be created if it does not exist.",
						required = True)
	parser.add_argument("-f", "--filetype",
						help = "Malware file type to read: 'bytes' or 'asm'.",
						required = True)

	# Get the arguments as a namespace.
	args = parser.parse_args()
	
	# Validate and parse filetype.
	if args.filetype not in ["bytes", "asm"]:
		raise BadFiletypeError("Bad --filetype flag: " + args.filetype + " is invalid. Must be once of: bytes, asm.")
	
	# Create base url for the uga-dsp bucket. The literal below is the GCP base address for malware data (both asm and bytecode files).
	extension = ".bytes" if args.filetype == "bytes" else ".asm"
	base_url = os.path.join(r"https://storage.googleapis.com/uga-dsp/project1/data/", args.filetype)

	# Fetch!
	fetchFiles(args.filenames, args.outdir, base_url, extension)







