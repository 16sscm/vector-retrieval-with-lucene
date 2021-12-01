import java.io.IOException;
import java.nio.file.Paths;

import com.hiretual.search.utils.GlobalPropertyUtils;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

public class IndexReaderTest {
    String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    // @Test
    public void getDocUid() throws IOException{

        IndexReader indexReader=DirectoryReader.open(MMapDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
        int size=1000;
        
        for(int i=0;i<size;i++){
            Document doc=indexReader.document(i);
            System.out.println(doc.get("uid"));
        }
       
    }
}
