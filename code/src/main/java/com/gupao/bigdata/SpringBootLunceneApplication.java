package com.gupao.bigdata;

import com.gupao.bigdata.lucene.IndexUse;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@SpringBootApplication
@Configuration
public class SpringBootLunceneApplication {
    @RequestMapping("search")
    @ResponseBody
    public List search(String q) throws Exception {
        //        索引文件将要存放的位置
       String indexDir = "D:\\lucenetemp\\lucene\\demo1";
        return IndexUse.search(indexDir, q);
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootLunceneApplication.class, args);
    }
}
