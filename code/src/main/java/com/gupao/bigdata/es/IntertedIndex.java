package com.gupao.bigdata.es;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
 
 //第一步 原生java创建索引
public class IntertedIndex {
	
	private Map<String, ArrayList<String>> map=new HashMap<>();
	private ArrayList<String> list;
	private Map<String, Integer> nums=new HashMap<>();
	
	public void CreateIndex(String filepath){
 
		String[] words = null;
		try {
		
			File file=new File(filepath);
			BufferedReader reader=new BufferedReader(new FileReader(file));
			String s=null;
			while((s=reader.readLine())!=null){
				//获取单词
				words=s.split(" ");
				
			}
			
			for (String string : words) {
			
				if (!map.containsKey(string)) {
					list=new ArrayList<String>();
					list.add(filepath.substring(filepath.lastIndexOf("\\")+1,filepath.lastIndexOf(".txt")));
					map.put(string, list);
					nums.put(string, 1);
				}else {
					list=map.get(string);
					//如果没有包含过此文件名，则把文件名放入
					if (!list.contains(filepath.substring(filepath.lastIndexOf("\\")+1,filepath.lastIndexOf(".txt")))) {
						list.add(filepath.substring(filepath.lastIndexOf("\\")+1,filepath.lastIndexOf(".txt")));
					}
					//文件总词频数目
					int count=nums.get(string)+1;
					nums.put(string, count);
				}
			}
			reader.close();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	
		
	}
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
	public static void charOutStream(String path) throws Exception{
		// 1：利用File类找到要操作的对象
		File file = new File(path);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}

		//2：准备输出流
		Writer out = new FileWriter(file);
		out.write("测试字符流, 哈哈");
		out.close();

	}

	public static void byteOutStream(String path,String content) throws Exception {

		//1:使用File类创建一个要操作的文件路径
//		File file = new File("D:" + File.separator + "demo" + File.separator + "test.txt");
		File file = new File(path);
		if(!file.getParentFile().exists()){ //如果文件的目录不存在
			file.getParentFile().mkdirs(); //创建目录

		}

		//2: 实例化OutputString 对象
		OutputStream output = new FileOutputStream(file);

		//3: 准备好实现内容的输出

		//将字符串变为字节数组
		byte data[] = content.getBytes();
		output.write(data);
		//4: 资源操作的最后必须关闭
		output.close();

	}
	private static ArrayList<String> filelist = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
			String filePath = "C:\\Users\\26853\\Pictures\\aclImdb_v1\\aclImdb\\test\\neg";
//		String filePath=args[0];
		getFiles(filePath);
		long start = System.currentTimeMillis();
		IntertedIndex index=new IntertedIndex();
		StringBuffer content1 = new StringBuffer();
		StringBuffer content2 = new StringBuffer();

		for(int i=0;i<filelist.size();i++){
			//String path="E:\\data\\"+i+".txt";
			index.CreateIndex(filelist.get(i));
		}
		long end = System.currentTimeMillis();
		System.out.println("一共花费了"+(end-start)+"时间");
		for (Map.Entry<String, ArrayList<String>> map : index.map.entrySet()) {
			System.out.println(map.getKey()+":"+map.getValue());
			content1.append(map.getKey()+":"+map.getValue()+"\n");
		}
//		byteOutStream("D:\\testdata\\result8.txt", content1.toString());
		byteOutStream(filePath+"\\result1.txt",content1.toString());

		for (Map.Entry<String, Integer> num : index.nums.entrySet()) {
			System.out.println(num.getKey()+":"+num.getValue());
			content2.append(num.getKey()+":"+num.getValue()+"\n");
		}
//		byteOutStream("D:\\testdata\\result9.txt", content2.toString());
		byteOutStream(filePath+"\\result2.txt", content2.toString());
		System.out.println("一共花费了"+(end-start)+"时间");
	}
}