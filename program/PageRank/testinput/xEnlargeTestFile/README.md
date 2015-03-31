# Usage #

test tool for file split.

in `makeLargeFile.cpp`

```
const int copyRatio = 3;
```

make `16x` size for test file.

```
g++ makeLargeFile.cpp -o enlarge 
```

```
./enlarge <testHtml.txt >duplicate.txt
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

## Sample Output(duplicate.txt) ##

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

