import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

import com.hiretual.search.utils.GlobalPropertyUtils;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

public class IndexReaderTest {

    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    // @Test
    public void getDocUid() throws IOException{

        IndexReader indexReader=DirectoryReader.open(MMapDirectory.open(Paths.get( INDEX_FOLDER)));
        int size=1000;
        int max=indexReader.maxDoc();
        int total=indexReader.numDocs();
        assertEquals(max, total);
        Random random=new Random();
        long t=System.currentTimeMillis();
        for(int i=0;i<size;i++){
            int index=random.nextInt(max);
            System.out.println(index);
            Document doc=indexReader.document(index);
            String raw=doc.get("raw_json");
            System.out.println(raw);
           
        }
        long end=System.currentTimeMillis();
        long cost =end-t;
        System.out.println(cost);
    }
}
