import java.io.IOException;
import com.hiretual.search.service.IndexBuildService;

import com.hiretual.search.utils.JedisUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexServiceTest {
    Logger logger =
    LoggerFactory.getLogger(IndexServiceTest.class);
    @Test
    public void testJedisUtils(){
      JedisUtils jedisUtils=new JedisUtils();
      
      jedisUtils.close();
    }
    @Test
    public void testReader() throws IOException{
       
         IndexBuildService indexBuildService=new IndexBuildService();
         indexBuildService.mergeSegments();
        
    }
    
}
