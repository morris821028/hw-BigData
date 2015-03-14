參照 [筆記-Hadoop Eclipse 開發環境建置 Hadoop-0.21 以及新增專案](http://blog.xuite.net/dain198/study/46416858-%E7%AD%86%E8%A8%98-Hadoop+Eclipse+%E9%96%8B%E7%99%BC%E7%92%B0%E5%A2%83%E5%BB%BA%E7%BD%AE+Hadoop-0.21+%E4%BB%A5%E5%8F%8A%E6%96%B0%E5%A2%9E%E5%B0%88%E6%A1%88)

# Eclipse #

## Step 1 ##

下載 `hadoop-0.20-2`，並且解壓縮。

去官網 Download，或者搜尋 `hadoop 0.20 dowload`。

## Step 2 ##

將 `~/Desktop/hadoop-0.20-2/contrib/eclipse-plugin/hadoop-0.20.2-eclipse-plugin.jar` 複製到 `eclipse/plugins` 下。

如果是 mac 使用者，下 command，將桌面的 jar 複製過去。

```
$ cd /Applications/eclipse/plugins
$ cp ~/Desktop/hadoop-0.20.2-eclipse-plugin.jar .
```

## Step 3 ##

重新啟動 Eclipse 後，可以得到 `File > Project... > New Project > Map/Reduce Project`，那麼就表示安裝成功。

選擇建立一個 Map/Reduce Project，接著會提示要給予 Hadoop library location。在選項 `Use default Hadoop (currently not set)` 的右方有藍色的 `Configure Hadoop install directory ...`，點選後設置 `Hadoop installation directory: ` 為剛剛下載的 Hadoop 0.20 解壓縮資料夾。

接著就可以著手你的 Map/Reduce 操作了！

## Step 4 ##

宣告 `Mapper` 和 `Reducer`，可以直接選擇 `Map/Reduce` 提供的預設 class。

# Unit Test #

如果要使用 unit test，來測試你的 Mapper 和 Reducer 的單一操作是否正確，藉由 MRUnit 這套軟體來完成，MRUnit 是專門針對 Map/Reduce 操作設計的。

此外可以額外下載 `Mockito`，為了宣告一個 object，可能需要很多參數來驅動，但測試時只會用到某些獨立的 function，那麼就可以使用 mock 來掛載 `.class` 檔案。

範例為 JUnit4 + MRUnit。

很簡單地，Mapper 會讀入一行的資訊，並且產生好幾個 `pair(type_key, type_val)`，為 `mapDriver` 預設輸出的 `pair`，注意，放入預設的順序跟產出的順序並須相同。

接著呼叫 `mapDriver.runTest()`，就會進行 `assertEqual()` 的判定！並且會告訴你預期結果和實際結果到底差別在哪裡。

同樣地，可以參考 Reducer 的寫法。

```
public class PageRankTest {

	PageRank.PageRankMapper mapper;
	PageRank.PageRankReducer reducer;
	Context context;
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() throws Exception {
		mapper = new PageRank.PageRankMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>()
				.withMapper(mapper);

		reducer = new PageRank.PageRankReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>()
				.withReducer(reducer);
		context = mock(Context.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapper() throws IOException, InterruptedException {
		mapDriver.withInput(new LongWritable(1L), new Text("A 1 B C D"));
		mapDriver.withOutput(new Text("A"), new Text("B"));
		mapDriver.withOutput(new Text("B"), new Text("0.3333333333333333"));
		mapDriver.withOutput(new Text("A"), new Text("C"));
		mapDriver.withOutput(new Text("C"), new Text("0.3333333333333333"));
		mapDriver.withOutput(new Text("A"), new Text("D"));
		mapDriver.withOutput(new Text("D"), new Text("0.3333333333333333"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException, InterruptedException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("A"));
		values.add(new Text("0.3333333333333333"));
		reduceDriver.withInput(new Text("B"), values);
		reduceDriver.withOutput(new Text("B"), new Text("0.43333333333333324 A"));
		reduceDriver.runTest();
	}
}
```