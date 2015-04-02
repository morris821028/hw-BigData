# 格式 #

解析每一個網站有很多方法，但是由於每個網站的 html 雜燴在同一個 file 中，導致解析問題的存在。

> hadoop 內建 `readline()` 支持跨檔讀取，同時也支持跨數個檔案讀取。 這一點可以從 hadoop MRunit 中測試得到。

回過頭來探討

## 案例一 ##

假設使用 url 作為重複 `if <begin url> = <end url>` 的判定，亦即讀取判斷為

```
<begin url>
... html
<end url>
```

遇到下列 html 格式，則會造成解析錯誤，會提早 match，而造成切分上的錯誤。

```
<html>
    <head>
        <title>WTF test</title>
    <head>
    <body>
        You know pre ?
<pre>
http://wtf.com
</pre>
    </body>
</html>
```

在助教的爬蟲結果，可能儲存為

```
http://wtf.com
<html>
    <head>
        <title>WTF test</title>
    <head>
    <body>
        You know pre ?
<pre>
http://wtf.com
</pre>
    </body>
</html>
http://wtf.com

... more
```

## 案例二 ##

假設用 one pass 的方式，進行 `<html>`之類的判定，有可能遭遇到下列代碼而造成分析錯誤。

```
<html>
    <head>
        <title>WTF2 test</title>
    <head>
    <body>
        You know script ?
<script>
var t = '\
</html> \
';
</script>
    </body>
</html>
```

## 案例三 ##

有人會考慮使用空行做為切割依據，但是有些網站會增加很多空白行，來防止爬蟲到內部資料，更有可能是本身設計時就存在的空白行。

```
<html>
    <head>
        <title>WTF2 test</title>
    <head>
    <body>
        empty line in html ?
<script>
// empty line

</script>
    </body>
</html>
```

在 15GB 的檔案中，不確定會不會有以上或者是其他的 html 寫法。


## Make Custom InputFormat ##

```
InputFormat.RecordReader<K,V> createRecordReader(InputSplit, TaskAttemptContext)
    |
    v
return new RecordReader()
```

[doc](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/InputFormat.html) 中提到 The framework will call RecordReader.initialize(InputSplit, TaskAttemptContext) before the split is used. 但是卻沒說 `initialize()` 到底何時被呼叫。

看 `RecordReader.initialize()` 到底做了什麼事情。

拿個 `LineRecordReader.class` 來查閱，支持以行為資料儲存的文檔。查閱 [source code LineRecordReader.class](http://grepcode.com/file/repository.cloudera.com/content/repositories/releases/com.cloudera.hadoop/hadoop-core/0.20.2-737/org/apache/hadoop/mapreduce/lib/input/LineRecordReader.java#53)

```
54  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
...

59  start = split.getStart();
60  end = start + split.getLength();
...

73 if (start != 0) {
74      skipFirstLine = true;
```

其中 `split.getStart()` 充分顯示了需要被通知起始位置，而在每一個 `local data file` 的起始位置 `offset = 0`，當某一行跨越檔案時，則藉由 `offset != 0` 來避免碎片化的第一行被當作一行資料。

```
0123456789
---------
AAAAAAAAAA   <--- skipFirstLine = false
AAAAAAAAAA
AAAAAAAAAA
---------
AAAAAAAAAA$  <--- skipFirstLine = true
BBBBBBBBBB
BBBBBBBBBB
BBBBBBBBBB$
```

藉由 `LineReader.readline()` 可以將 `A` 讀完。並且讓另一個位置從 B 的地方開始讀取。

在 `RecordReader.class` 下，都是藉由 `RecordReader.nextKeyValue()` 去命令解析下一筆資料，這時候才將當前的 parser offset 往後移動。

## 問題 ##

是否在這樣的情況下，呈現序列化的呼叫，`RecordReader.class` 必須要等待 `initialize()` 後才知道後否要忽略已被處理的資料。延遲會隨著檔案切割的數量拉長？

答案是不會的，Mapper 各自的 RecordReader 都平行運行，而要有能力去判斷是否為碎片資料進行忽略。從 [split into multiple lines](http://stackoverflow.com/questions/17713476/how-to-read-a-record-that-is-split-into-multiple-lines-and-also-how-to-handle-br) 的關鍵字下手，找到一個比較接近的 `XMLRecordReader`，而內建的 `NLineRecordReader` 並不符合這一個變量行數的操作，所以並無參考價值。

從 `XMLRecordReader` 的編寫方式中，了解到每一個 Mapper 在讀取檔案時，都會去搜索第一個 `<Start_Tag>`，也就是說 XML 具有一個獨特的 `<Start_Tag>` 作為某筆資料開始標記，並不存在資料內容中出現 `<Start_Tag>`，否則在切割某筆資料會產生誤判。

```
<tail_tag1> <---- first line in FileSplit
<tail_tag2>
<tail_tag3>
<End_Tag>

<Start_Tag> <---- if match <Start_Tag>, store context until math <End_Tag> 
...

<End_Tag>   <---- match <End_Tag>, value = this.block, return true <key, value>, pos = file offset.
```

原本的格式是否無解，無法確定。實作自己的 `BlockRecordReader` 後，由於不像 `XML <Start_Tag>` 獨特。第一行 URL 就會當作 `<Start_Tag>`，簡單流程如下。

```
while (pos < end)
	while (true)
		v = readline(pos)
		if (v is URL) // maybe occur replace
			/*
				replace

				## Case 1: ##

				<p>
				------ split file 
				http://wtf1.com$ 	<---- fail start_tag !!
				</p>
				...
				</html>

				http://wtf2.com$
			 */
			if pos - start_tag >= end // in next split
				return false
			start_tag = v
		else
			/* 
				maybe `<!-- --> start` or split in tricky case
				
				## Case 1: ##

				http://wtf1.com$
				$
				http://wtf2.com$
				<html>

				## Case 2: ##

				http://wtf2.com$
				<!DOCTYPE html>

				## Case 3: ##

				http://wtf2.com$
				<!--[if lt IE8]><html><![endif]-->$
				<html>

			 */
			if v has `<??? html ???>` but not </html>
				break;

	while (true) // read html block
		v = readline(pos)
		if (v.match(start_tag))
			return true;
```


## 避免 `<tag>` 碰撞 ##

轉換編碼格式 (如 Base64) 儲存，放在 XML 或者是其他儲存結構，這麼以來 HTML 的問題就能大幅度地減少，需要的時候進行解碼即可。