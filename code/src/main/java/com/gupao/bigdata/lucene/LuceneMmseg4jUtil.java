package com.gupao.bigdata.lucene;
 
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;
 
import com.chenlb.mmseg4j.analysis.MMSegAnalyzer;

/**
 * 基于MMSegAnalyzer分词器,磁盘存储的搜索引擎
 * 
 * @author Administrator
 *
 */
public class LuceneMmseg4jUtil {
 
	// 分词器
	Analyzer analyzer = new MMSegAnalyzer();
	// 存放索引的路径
	String direct = "D:\\index";
	String dataDir = "C:\\Users\\26853\\Pictures\\aclImdb_v1\\aclImdb\\test\\neg";
	/**
	 * 创建索引并进行存储
	 * 
	 * @param title
	 * @param content
	 * @throws IOException
	 */
	public void index() throws IOException {

		Directory directory = FSDirectory.open(Paths.get(direct));
		IndexWriterConfig iwConfig = new IndexWriterConfig(analyzer);
		// 设置创建索引模式(在原来的索引的基础上创建或新增)
		iwConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
		/*
		 * 添加索引，在之前的索引基础上追加 
		 * iwConfig.setOpenMode(OpenMode.APPEND); 
		 * 创建索引，删除之前的索引
		 * iwConfig.setOpenMode(OpenMode.CREATE);
		 */
		IndexWriter iwriter = new IndexWriter(directory, iwConfig);
		// 创建一个存储对象
		Document doc = new Document();
		// 添加字段
		File[] files = new File(dataDir).listFiles();
		for (File file : files) {
			doc.add(new TextField("title", file.getName(), Field.Store.YES));
			doc.add(new TextField("content", new FileReader(file)));
			// 新添加一个doc对象
			iwriter.addDocument(doc);
			// 创建的索引数目
			int numDocs = iwriter.numDocs();
			System.out.println("共索引了: " + numDocs + " 个对象");
		}

		// 提交事务
		iwriter.commit();
		// 关闭事务
		iwriter.close();
 
	}



	/**
	 * 高亮处理
	 * 
	 * @param query
	 * @param fieldName
	 * @param fieldContent
	 * @return
	 * @throws IOException
	 * @throws InvalidTokenOffsetsException
	 */
	public String displayHtmlHighlight(Query query, String fieldName, String fieldContent)
			throws IOException, InvalidTokenOffsetsException {
		// 设置高亮标签,可以自定义,这里我用html将其显示为红色
		SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<font color='red'>", "</font>");
		// 评分
		QueryScorer scorer = new QueryScorer(query);
		// 创建Fragmenter
		Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
		// 高亮分析器
		Highlighter highlight = new Highlighter(formatter, scorer);
		highlight.setTextFragmenter(fragmenter);
		// 调用高亮方法
		String str = highlight.getBestFragment(analyzer, fieldName, fieldContent);
		return str;
	}
 
	/**
	 * 查询方法
	 * 
	 * @param text
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws InvalidTokenOffsetsException
	 */
	public List<Map<String, Object>> search(String text)
			throws IOException, ParseException, InvalidTokenOffsetsException {
 
		// 得到存放索引的位置
		Directory directory = FSDirectory.open(Paths.get(direct));
		DirectoryReader ireader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(ireader);
		// 在content中进行搜索
		QueryParser parser = new QueryParser("content", analyzer);
		// 搜索含有text的内容
		Query query = parser.parse(text);
		// 搜索标题和显示条数(10)
		TopDocs tds = searcher.search(query, 10);
 
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
 
		// 在内容中查获找
		for (ScoreDoc sd : tds.scoreDocs) {
			// 获取title
			String title = searcher.doc(sd.doc).get("title");
			// 获取content
			String content = searcher.doc(sd.doc).get("content");
			// 内容添加高亮
			QueryParser qp = new QueryParser("content", analyzer);
			// 将匹配到的text添加高亮处理
			Query q = qp.parse(text);
			String html_content = displayHtmlHighlight(q, "content", content);
 
			map = new HashMap<String, Object>();
			map.put("title", title);
			map.put("content", html_content);
			list.add(map);
		}
 
		return list;
	}
 
	/**
	 * 删除索引方法
	 * 
	 * @param filed
	 * @param keyWord
	 * @throws IOException
	 */
	public void delete(String filed, String keyWord) throws IOException {
 
		Directory directory = FSDirectory.open(Paths.get(direct));
		IndexWriterConfig iwConfig = new IndexWriterConfig(analyzer);
		// 在原来的索引的基础上创建或新增
		iwConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
		IndexWriter iwriter = new IndexWriter(directory, iwConfig);
		// 删除filed中含有keyWord的索引
		iwriter.deleteDocuments(new Term(filed, keyWord));
		// 提交事务
		iwriter.commit();
		// 关闭事务
		iwriter.close();
 
	}
 
	@Test
	// 创建索引测试
	public void indexTest() throws IOException {
		LuceneMmseg4jUtil lucene = new LuceneMmseg4jUtil();
		lucene.index();
	}
 
	@Test
	// 查询测试
	public void searchTest() throws IOException, ParseException, InvalidTokenOffsetsException {
		LuceneMmseg4jUtil lucene = new LuceneMmseg4jUtil();
		List<Map<String, Object>> list = lucene.search("网络工程");
		for(Map<String,Object> map:list) {
			System.out.println(map);
		}
	}
 
	@Test
	// 删除索引测试
	public void deleteTest() throws IOException {
		LuceneMmseg4jUtil lucene = new LuceneMmseg4jUtil();
		lucene.delete("title", "简介");
	}
 
}