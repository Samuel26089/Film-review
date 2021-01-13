package com.gupao.bigdata.es;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
// hadoop mapreduce创建索引
public class Index {
    private static ArrayList<String> filelist = new ArrayList<String>();

    /*
     * 通过递归得到某一路径下所有的目录及其文件
     */
    static void getFiles(String filePath) {
        File root = new File(filePath);
        File[] files = root.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                /*
                 * 递归调用
                 */
                getFiles(file.getAbsolutePath());
                filelist.add(file.getAbsolutePath());
                // System.out.println("显示"+filePath+"下所有子目录及其文件"+file.getAbsolutePath());
            } else {
                System.out.println("@" + file.getAbsolutePath() + ";");
                filelist.add(file.getAbsolutePath());
            }
        }
    }
    private static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException, IOException {
            // 可以获取文件名，根据文件名来判定传入reducer的形式
            CombineFileSplit cfs = (CombineFileSplit)context.getInputSplit();
//            String fileA = cfs.getPath(0).getName();
//            String fileB = cfs.getPath(1).getName();
            for (Path path:cfs.getPaths()){
//                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//                System.out.println(fileName);
                String[] strs = value.toString().trim().split(" ");
                for(int i=0; i<strs.length; i++){
                    context.write(new Text(strs[i]), new Text(path.getName()));
                }
            }

        }
    }

    private static class IndexReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text value, Iterable<Text> datas, Reducer<Text, Text, NullWritable, Text>.Context context)  throws IOException, InterruptedException {
            String resultStr="";
            for(Text data:datas){
                String[] strs = data.toString().split("[.]");
                String[] res = resultStr.split(",");
                if(!strs[0].equals(res[res.length-1])){
                    resultStr+=strs[0]+",";
//                    resultStr = resultStr.substring(0, resultStr.length()-1);
                }
            }
            context.write(NullWritable.get(), new Text(value.toString()+":"+resultStr));
        }
    }
    
    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            BasicConfigurator.configure();
            Job job = Job.getInstance(conf, "index");
            job.setJarByClass(Index.class);
            job.setMapperClass(IndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(IndexReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
//            for (int index=0;index<=filelist.size();index++){
//                filelist.get(index);
//            }
            FileInputFormat.addInputPath(job, new Path("C:\\Users\\26853\\Pictures\\aclImdb_v1\\aclImdb\\test\\neg"));
//            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path("D://output12345678910"));
//            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            long start = System.currentTimeMillis();
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            long end = System.currentTimeMillis();
            System.out.println("一共花费了"+(end-start)+"时间");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}