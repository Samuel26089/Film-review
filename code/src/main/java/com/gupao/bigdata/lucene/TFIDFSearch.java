package com.gupao.bigdata.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;

/**
 * 配合Demo2.java进行lucene的helloword实现
 * @author Administrator
 *
 */
public class TFIDFSearch {
    /**
     * 通过关键字在索引目录中查询
     *
     * @param indexDir 索引文件所在目录
     * @param q        关键字
     */
    public static void search(String indexDir, String q) throws Exception {
        FSDirectory indexDirectory = FSDirectory.open(Paths.get(indexDir));
//        注意:索引输入流不是new出来的，是通过目录读取工具类打开的
        IndexReader indexReader = DirectoryReader.open(indexDirectory);
//        获取索引搜索对象
        IndexSearcher searcher = new IndexSearcher(indexReader);
        //TF IDF 算法
        searcher.setSimilarity(new ClassicSimilarity());//切换算法
        TermQuery qq = new TermQuery(new Term("contents", q));
        BoostQuery bq = new BoostQuery(qq, 2f);
        //去最高的的十个
        TopDocs topDocs = searcher.search(bq, 10);
        System.out.println("totalHits:" + topDocs.totalHits + ",MaxScore" + topDocs.getMaxScore());
        //输出结果
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            Document doc = searcher.doc(scoreDoc.doc);
            System.out.print("文件名称"+doc.get("fileName"));
            System.out.print("               ");
            System.out.print("IFIDF打分结果"+scoreDoc.score);
            System.out.print("               ");
            //打分计算详情输出
            System.out.println( " 打分计算详情输出: \n" + (searcher.explain(bq, scoreDoc.doc)));
        }
            indexReader.close();
            indexDirectory.close();
    }
}