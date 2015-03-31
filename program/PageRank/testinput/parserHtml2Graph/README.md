# Usage #

* pathon 2.7

`dataParser.py` and `testHtml.txt` in the same document, then output `linkGraph.txt`

```
$ python dataParser.py
```

## Sample Input(testHtml.txt) ##

```
https://tw.yahoo.com/
<html>
<head></head>
<body>
<a href='http://www.w3schools.com'>W3Schools</a>
<a href='http://www.google.com'>Google</a>
</body>
</html>
https://tw.yahoo.com/


https://www.youtube.com/
<html>
<head></head>
<body>
<a href='http://www.w3schools.com'>W3Schools</a>
<a href='http://www.google.com'>Google</a>
<a href="https://tw.yahoo.com/">Yahoo</a>
</body>
</html>
https://www.youtube.com/

```

## Sample Output(linksGraph.txt) ##

```
https://tw.yahoo.com/ 1 http://www.w3schools.com/ http://www.google.com/
https://www.youtube.com/ 1 http://www.w3schools.com/ https://tw.yahoo.com/ http://www.google.com/
```

## Stdout ##

```
morrisdeMacBook-Air:parserHtml2Graph morris$ python dataParser.py 
store :https://tw.yahoo.com/

store :https://www.youtube.com/
```
