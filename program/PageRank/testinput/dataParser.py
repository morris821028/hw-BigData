import re
import sys
from HTMLParser import HTMLParser
from urlparse import urlparse

reload(sys)
sys.setdefaultencoding('utf8')

urlRegex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

def isUrl(url):
	try:
		return urlRegex.match(url)
	except:
		return False

def getDomainFromUrl(url):
	url = urlparse(url)
	domain = '{uri.scheme}://{uri.netloc}/'.format(uri=url)
	return domain

class MyHTMLParser(HTMLParser):  
	def __init__(self):
		self.linkSet = set()
		HTMLParser.__init__(self) 

	def handle_starttag(self, tag, attrs):
		if tag == 'a':
			for name, value in attrs:
				if name == 'href' and isUrl(value):
					self.linkSet.add(getDomainFromUrl(value))

class DataParser:
	def __init__(self, filename, outFilename):
		self.filename = filename
		self.fout = open(outFilename, "w")

	def storeGraph(self, site, html):
		parser = MyHTMLParser()  
		parser.feed(html)  
		site = getDomainFromUrl(site)
		elem = site + ' 1'
		
		if site in parser.linkSet:
			parser.linkSet.remove(site)
		for domain in parser.linkSet:
			elem += ' ' + domain

		self.fout.write(elem + '\n')

	def open(self):
		f = open(self.filename)
		html = ''
		startSite = False
		startSiteUrl = ''

		for line in f:
			if startSite == False and isUrl(line):
				html = ''
				startSite = True
				startSiteUrl = line
			elif startSite == True:
				if line == startSiteUrl and isUrl(line):
					self.storeGraph(startSiteUrl, html)
					html = ''
					startSite = False
					startSiteUrl = ''
				else:
					html = html + line + '\n'
				

if __name__ == '__main__':
	parser = DataParser('testHtml.txt', 'linkGraph.txt')
	parser.open()