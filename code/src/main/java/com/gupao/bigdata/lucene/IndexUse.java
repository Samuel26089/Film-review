package com.gupao.bigdata.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 配合Demo2.java进行lucene的helloword实现
 * @author Administrator
 *
 */
public class IndexUse {
    /**
     * 通过关键字在索引目录中查询
     * @param indexDir    索引文件所在目录
     * @param q    关键字
     */
    public static List search(String indexDir, String q) throws Exception{
        List list= new ArrayList<>();
        FSDirectory indexDirectory = FSDirectory.open(Paths.get(indexDir));
//        注意:索引输入流不是new出来的，是通过目录读取工具类打开的
        IndexReader indexReader = DirectoryReader.open(indexDirectory);
//        获取索引搜索对象
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        //TF IDF 算法
        indexSearcher.setSimilarity(new ClassicSimilarity());//切换算法
        TermQuery qq = new TermQuery(new Term("contents", q));
        BoostQuery bq=new BoostQuery(qq,2f);

        Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("contents", analyzer);
//        获取符合关键字的查询对象
        Query query = queryParser.parse(q);

        long start=System.currentTimeMillis();
//        获取关键字出现的前十次
        TopDocs topDocs = indexSearcher.search(query , 10);
        long end=System.currentTimeMillis();
        System.out.println("匹配 "+q+" ，总共花费"+(end-start)+"毫秒"+"查询到"+topDocs.totalHits+"个记录");

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int docID = scoreDoc.doc;
//            索引搜索对象通过文档下标获取文档
            Document doc = indexSearcher.doc(docID);
            System.out.println(q+"："+doc.get("fileName"));
            list.add(q+doc.get("fileName"));
        }

        indexReader.close();
        return list;
    }
}