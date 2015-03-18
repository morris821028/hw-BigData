## Big Data ##

### Start ###

要使用 hadoop 的環境，由裝有 hadoop 的主機替我們進行 Map/Reduce 的工作。

使用 ssh 進行遠端登入，由於 Mac 上沒有 Xshell 這麼方便的軟體，仍然可以使用 command line 模式進行連線。假設主機的 ip 為 `140.115.XX.XX`，指定的 port 為 `22`，使用者帳號為 `s100502205`。

```
morrisdeMacBook-Air:~ morris$ ssh s100502205@140.115.XX.XX -p 22
```

變更登入遠端的使用者登入密碼，此步驟可忽略

```
s100502205@node1:~$ passwd
```

現在正在遠端電腦的 home 位置，這個目錄下都是空的。接著去根目錄下複製 hadoop 下來。先建立一個這次測試的資料夾。

```
s100502205@node1:~$ cd ~
s100502205@node1:~$ mkdir test1 
s100502205@node1:~$ cd ~/test1 
s100502205@node1:~$ mkdir wc
```

將助教寫好的 `WordCount.java` 和測試檔案從根目錄抓到自己的資料夾下。

```
$ cp /hadoop/wc/WordCount.java ~/test1/wc 
$ cp /hadoop/wc/data.txt ./
```

接著嘗試編譯我們的 Map/Reduce Project `WordCount.java` 變成 `.class`。

```
$ cd ~/test1/wc
$ mkdir class
```

編譯撰寫的 Map/Reduce java 檔，下方指令處理單一 java 檔的編譯，並且產出 jar 檔案。

```
$ jar -cvf WordCount.jar -C class ./
$ javac -classpath /opt/hadoop/lib/commons-cli-1.2.jar:/opt/hadoop/lib/hadoop-core-0.20.2+320.jar -d class WordCount.java
```

每一次登入遠端電腦時，都必須掛載 hadoop 的環境，將 hadoop 建置的腳本 script 藉由 source 指令來執行，

```
source /opt/hadoop/conf/hadoop-env.sh
```

將本地的輸入資料，放入 hadoop 系統，並且命名為 input。

```
$ hadoop fs -put ~/test1/data.txt input
```

接著執行藉由 hadoop 操作，來執行我們的 `WordCount.jar`，設定 main function 的所在 package example 下的 `WordCount.class`，輸入輸出的參數分別是 `input` 和 `output`。

輸出會在 `output` 這個資料夾下。

```
$ hadoop jar WordCount.jar example.WordCount input output
```

將 hadoop 的 output 資料下的所有結果，複製到本地的 moutput 目錄下。因為 Map/Reduce 會將檔案切割成好幾個，命名為 `part-r-00000` 依次編號。直接整個抓下來！

```
$ hadoop fs -get output moutput
```

複製完後，就可以將 hadoop 的 output 資料夾整個刪除。

```
$ hadoop fs –rmr output
```

接著查閱程式有沒有正確，可以利用 `vim` 編輯器來觀看

```
$ vim output/part-r-00000
```

如果看完，想要刪除整個 output 資料夾，可以藉由 `rm` 完成。

```
$ rm -r output
```

### Write ###

現在要開始撰寫自己的 `PageRank.java`，如果不想用 `vim` 來進行編輯，在本地端編輯後，使用 `scp` 指令將檔案傳上去

```
morrisdeMacBook-Air:~ morris$ scp ~/Desktop/PageRank.java s100502205@140.115.XX.XX:~/pagerank/code
```

切記，當前位置是在自己的電腦，而不是 ssh 連線後的 `s100502205@node1:~$`。

接著就是掛載環境、編譯、打包的連續步驟。

```
$ source /opt/hadoop/conf/hadoop-env.sh
$ javac -classpath /opt/hadoop/lib/commons-cli-1.2.jar:/opt/hadoop/lib/hadoop-core-0.20.2+320.jar  -d class PageRank.java -Xlint
$ jar -cvf PageRank.jar -C class ./
```

然後就是等產出囉！

```
$ hadoop jar PageRank.jar main.PageRank input output
```

```
$ javac -classpath /opt/hadoop/lib/commons-cli-1.2.jar:/opt/hadoop/lib/hadoop-core-0.20.2+320.jar  -d class *.java -Xlint
$ jar -cvf PageRankCustom.jar -C class ./
$ jar PageRankCustom.jar main.PageRankCustom /user/data/htm100.txt output
$ $ jar PageRankCustom.jar main.PageRank linksGraph output
```