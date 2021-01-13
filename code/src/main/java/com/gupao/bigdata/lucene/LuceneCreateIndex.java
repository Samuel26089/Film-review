package com.gupao.bigdata.lucene;

public class LuceneCreateIndex {
    public static void main(String[] args) {
//        索引文件将要存放的位置
        String indexDir = "D:\\lucenetemp\\lucene\\demo1";
//        String indexDir = args[0];

//        数据源地址
        String dataDir = "C:\\Users\\26853\\Pictures\\aclImdb_v1\\aclImdb\\test\\neg";
//        String dataDir=args[1];
        IndexCreate ic = null; 
        try {
            ic = new IndexCreate(indexDir);
            long start = System.currentTimeMillis();
            int num = ic.index(dataDir);
            long end = System.currentTimeMillis();
            System.out.println("索引指定路径下"+num+"个文件，一共花费了"+(end-start)+"毫秒");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                ic.closeIndexWriter();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}