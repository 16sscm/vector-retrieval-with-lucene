import java.io.IOException;

import com.hiretual.search.model.FilterQuery;
import com.hiretual.search.model.FilterResume;
import com.hiretual.search.service.IndexBuildService;

import com.hiretual.search.utils.JedisUtils;
import com.hiretual.search.utils.RequestParser;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexServiceTest {
    Logger logger =
    LoggerFactory.getLogger(IndexServiceTest.class);
    // @Test
    public void testJedisUtils(){
      JedisUtils jedisUtils=new JedisUtils();
      jedisUtils.get("tiq_205890fb-cc94-4da7-a246-bb1f9ff8df8e");
      
    }
    // @Test
    public void testResumeJsonStr(){
      FilterResume filterResumes[]=new FilterResume[3];
      filterResumes[0]=new FilterResume("aaa");
      filterResumes[1]=new FilterResume("bbb");
      filterResumes[2]=new FilterResume("ccc");
      String str=RequestParser.getJsonString(filterResumes);
      String[] black_uids=new String[]{"aaa"};
      FilterQuery filterQuery=new FilterQuery(black_uids);
      String filterString=RequestParser.getJsonString(filterQuery);
      System.out.println(str);
    }
    // @Test
    public void testReader() throws IOException{
       
         IndexBuildService indexBuildService=new IndexBuildService();
         indexBuildService.mergeSegments();
        
    }
    
}
