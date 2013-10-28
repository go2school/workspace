if __name__ == '__main__':	
	import sys
	from nltk.util import clean_html
	print sys.argv
	inHTML = sys.argv[1]
	outText = sys.argv[2]
	fd = open(inHTML)
	ct = fd.read()
	fd.close()
	text = clean_html(ct)
	fd = open(outText, 'w')
	fd.write(text)
	fd.close()
