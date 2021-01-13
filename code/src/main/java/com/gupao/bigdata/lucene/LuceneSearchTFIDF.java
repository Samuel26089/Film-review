package com.gupao.bigdata.lucene;

/**
 * 查询索引测试
 * @author Administrator
 *
 */
// 相关度 lucene的索引
public class LuceneSearchTFIDF {
    public static void main(String[] args) {
//        String indexDir = "D:\\lucenetemp\\lucene\\demo1";
        String indexDir=args[0];
//        String q = "between";
        String q=args[1];
        try {
            TFIDFSearch.search(indexDir, q);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}