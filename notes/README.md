# hw-BigData

## Start ##

使用 ssh 進行遠端登入，由於 Mac 上沒有 Xshell 這麼方便的軟體，仍然可以使用 command line 模式進行連線。

根據作業，主機的 ip 為 `140.115.XX.XX`，指定的 port 為 `22`，使用者帳號為 `s100502205`。

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
$ javac -classpath /opt/hadoop/lib/commons-cli-1.2.jar:/opt/hadoop/lib/hadoop-core- 0.20.2+320.jar -d class WordCount.java
```

接著包成 jar 檔案。

```
$ jar -cvf PageRank.jar -C class ./
```

取

```
source /opt/hadoop/conf/hadoop-env.sh
```

```
$ javac -classpath /opt/hadoop/lib/commons-cli-1.2.jar:/opt/hadoop/lib/hadoop-core-0.20.2+320.jar  -d class PageRank.java -Xlint
```

```
$ hadoop jar PageRank.jar main.PageRank input output
```