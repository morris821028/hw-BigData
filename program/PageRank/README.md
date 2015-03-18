# Format #

如果輸入資料的格式如下

```
A 1 B C D
B 1 C D
C 1 D
D 1 B
```

每一行為一個網站的連接資料，第二個元素為當前的 PageRank。

若寫為連接矩陣為

```
0111
0011
0001
0100
```

程式中，使用的 `beta = 0.85`、`sum(PageRank) = N`

因此上述的 PageRank 分別為

```
A 0.15 B C D
B 1.49 C D
C 0.83 D
D 1.53 B
```

如果發生測試資料如下

```
A 1 B C D
B 1 C D X
C 1 D Y
D 1 B 
```

則沒有出現過的 X、Y 不影響我們的計算，在 MapReduce 後，X Y 的紀錄就會跳出來，成為新的 Input，也許會長成這樣。

```
A 1.41 B C D
B 1.58 C D X
C 0.25 D Y
D 1.12 B 
X 0.11
Y 0.22
```

由於假設總和是 N (所有網站個數)，得到迭代 `r = beta M r + (1 - beta)`，計算時與 N 無關。

## Other Format Process ##

[Hadoop MapReduce中如何处理跨行 Block 和 UnputSplit](http://olylakers.iteye.com/blog/1070068)

現在還處於 TextInputFormat，也就是資料元素是以行 `'\n'` 為區隔，有沒有可能因為 chunk，導致同一行會被切割到不同的 Node 上面去運行？

從 source code 可以發現，這是有可能的，但這不影響程式的運行，由於 `readline()` 設計的相當好，當前主機若讀不到 `'\n'`，則會想辦法去抓完一整行，同時不會讓碎片尾端在另外一台主機成為新的一筆資料。

接著，萬一輸入格式是以空行為區隔怎麼辦？

### Reference ###

* [Hadoop : WordCount with Custom Record Reader of TextInputFormat](http://bigdatacircus.com/2012/08/01/wordcount-with-custom-record-reader-of-textinputformat/)

* [Hadoop: RecordReader and FileInputFormat](https://hadoopi.wordpress.com/2013/05/27/understand-recordreader-inputsplit/)

* [MRunit 测试](http://yugouai.iteye.com/blog/2161631)