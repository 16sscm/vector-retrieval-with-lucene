import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    @Autowired
  private JedisUtils jedisUtils;
    @Test
    public void testReader() throws IOException{
       
    //      String USER_HOME = System.getProperty("user.home");
    //     String INDEX_FOLDER =
    //         GlobalPropertyUtils.get("index.folder");
    //     IndexReader reader = DirectoryReader.open(
    //         FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
  
    //     List<LeafReaderContext> list = reader.leaves();
    //     if (list.size() > 1) {
    //         logger.error("error:more the one segment");
    //         return;
    //       }
    //       LeafReaderContext lrc = list.get(0);
    //       LeafReader lr = lrc.reader();
    //       int maxDocId = lr.maxDoc();
    //       int numDocs=lr.numDocs();
    //       if(maxDocId!=numDocs){
    //         logger.warn("stop add to jna,invalid document num for segment,numDocs: "+numDocs+",maxDoc:"+maxDocId);
    //         return;
    //       }else{
    //         logger.info(maxDocId+" documents to do clib index");
    //       }
    //       int docId = 413000;
    //       maxDocId=maxDocId-docId;
    //       int batchSize = 1000;
    //       int integralBatch = maxDocId / batchSize;
    //       int remainBatchSize = maxDocId % batchSize;
          
    //       for (int i = 0; i < integralBatch; i++) {
    //         addVectors(lr,batchSize,docId);
    //         docId+=batchSize;
    //       }
    //       if(remainBatchSize>0){
    //         addVectors(lr,remainBatchSize,docId);
    //         docId+=remainBatchSize;
    //       }
        
    // }
    }
}
