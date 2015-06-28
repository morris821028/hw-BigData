# Minhash #

計算 Minhash，使用 100 種不同的排列，使用同一種 hash 來完成。

```
read Attr[]

for i = 0 to 100
	copy test = Attr
	permutation_kind(i, test)
	for j = 0 to test.length
		if test[j] == 1
			signature[i] = min(signature[i], hash(j))

return signature
```

## Input Format ##

格式如下 `id##attribute`，用 0/1 表示是否具有第 i 種特徵，因此每一組資料都具有相同特徵長度。

```
000##00001000001.....
001##01010000011.....
002##01010000011.....
...
```

## Output ##

對於每一組資料輸出壓縮過後的 100 種 hash 結果。

```
000	da694514 bcc0ba94 bcc0ba94 da694514 2b2727a9 8721e329 da694514 da694514 da694514 da694514 bcc0ba94 da694514 da694514 2b2727a9 da694514 da694514 bcc0ba94 da694514 da694514 da694514 da694514 da694514 8721e329 da694514 bcc0ba94 bcc0ba94 da694514 da694514 8721e329 bcc0ba94 da694514 2b2727a9 da694514 954bf814 bcc0ba94 da694514 da694514 da694514 da694514 da694514 8721e329 bcc0ba94 da694514 2b2727a9 da694514 964c9a10 da694514 2b2727a9 da694514 da694514 bcc0ba94 5e0aa169 da694514 da694514 da694514 2b2727a9 2b2727a9 954bf814 da694514 da694514 da694514 bcc0ba94 da694514 2b2727a9 da694514 da694514 954bf814 954bf814 bcc0ba94 da694514 2b2727a9 bcc0ba94 5e0aa169 da694514 bcc0ba94 bcc0ba94 da694514 5e0aa169 bcc0ba94 bcc0ba94 da694514 bcc0ba94 da694514 da694514 da694514 da694514 da694514 da694514 5e0aa169 bcc0ba94 da694514 da694514 da694514 2b2727a9 da694514 da694514 da694514 2b2727a9 da694514 5e0aa169
...
```

## Test ##

檢測資料 `Minhash/testinput/test_example/test.cpp` 可以進行檢驗，檢驗方法為相似度 80% 的對數統計，套用 Local Sensitive Hash (LSH) 進行 `O(N^2 M)` 比較，其中 N 為資料筆數，M 為屬性種數，由於需要暴力檢測相似度，故複雜度限制在此，LSH 可以做得更快。

```
$ g++ test.cpp -o a.out
$ ./a.out input1.txt part-r-00000
```

產生測資的方法使用 `Minhash/testinput/test_example/pin.cpp`，編譯和產生方法如下：

```
$ g++ pin.cpp -o pin
$ ./pin >input.txt
```

## Notes ##

hadoop 使用時，不可隨機生成 hash，因為會分散到每一台不同的 node 去處理，除非亂數種子相同。可以用不同的 hash 取代排列運算。或者

