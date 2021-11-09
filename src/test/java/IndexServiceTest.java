import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.utils.JedisUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
