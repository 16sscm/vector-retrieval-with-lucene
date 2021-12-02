import com.hiretual.search.filterindex.*;
import com.hiretual.search.model.FilterQuery;
import com.hiretual.search.model.FilterResume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.utils.RequestParser;
import com.sun.jna.Pointer;
import java.util.Random;
import org.junit.Test;


public class ClibTest {
  String USER_HOME = System.getProperty("user.home");
  String c_index_dir = USER_HOME + GlobalPropertyUtils.get("c_index_dir");

  String pFlatFile = c_index_dir + GlobalPropertyUtils.get("flat_file");
  String pIvfpqFile = c_index_dir + GlobalPropertyUtils.get("ivfpq_file");
  // @Test
  public void testClib() {
    CLib cLib = CLib.INSTANCE;
    Random r = new Random();
    String msg = cLib.FilterKnn_GetErrorMsg();
    System.out.println(msg + "t");

    System.out.println("loading...");
    long t = System.currentTimeMillis();
    int suc =cLib.FilterKnn_InitLibrary(128, 10000, 64, 8, pIvfpqFile);
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));
    System.out.println(suc + "tt");

    int db = 3;
    float[] vectors = new float[128 * db];
    long[] id = new long[db];
    // long n=10;

    System.out.println("gendata...");
    t = System.currentTimeMillis();
    for (int i = 0; i < db; i++) {
      for (int j = 0; j < 128; j++) {
        vectors[i * 128 + j] = r.nextFloat();
      }
      id[i] = (long)i;
    }
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));

    System.out.println("adding data...");
    t = System.currentTimeMillis();
    FilterResume filterResumes[]=new FilterResume[3];
    filterResumes[0]=new FilterResume("aaa");
    filterResumes[1]=new FilterResume("bbb");
    filterResumes[2]=new FilterResume("ccc");
    String str=RequestParser.getJsonString(filterResumes);
    int suc1 = cLib.FilterKnn_AddVectors(vectors, id, db,str);
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));
    System.out.println(suc1 + "ttt");

    System.out.println("saving data...");
    t = System.currentTimeMillis();
    int suc2 = cLib.FilterKnn_Save( pIvfpqFile);
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));
    System.out.println(suc2 + "tttt");

    float[] query = new float[128];
    for (int i = 0; i < 128; i++) {
      query[i] = r.nextFloat();
    }
    int flat_db = 3;
    long[] flatId = new long[flat_db];
    for (int i = 0; i < flat_db; i++) {
      flatId[i] = i;
    }
    int flatSearchSize = 3;
    long resultIds[] = new long[flatSearchSize];
    float resultDistances[] = new float[flatSearchSize];
    System.out.println("flat search...");
    t = System.currentTimeMillis();
    String[] black_uids=new String[]{"aaa"};
    FilterQuery filterQuery=new FilterQuery(black_uids);
    String filterString=RequestParser.getJsonString(filterQuery);
    long suc3 = cLib.FilterKnn_FlatSearch(
        query, flatId, flat_db, 0,filterString, flatSearchSize, resultIds, resultDistances);
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));
    System.out.println(suc3 + "ttttt");

    int topK = 1000;
    long resultIds1[] = new long[topK];
    float resultDistances1[] = new float[topK];
    System.out.println("ivfpq search...");
    t = System.currentTimeMillis();
    long suc4 = cLib.FilterKnn_IvfpqSearch(query, 1000, 10000, 5, 0,null, topK,
                                           resultIds1, resultDistances1);
    System.out.println("done!cost:" + (System.currentTimeMillis() - t));
    System.out.println(suc4 + "tttttt");
  }
  @Test
  public void testPointer(){
    // CLib cLib = CLib.INSTANCE;
    // Pointer pointer=cLib.FilterKnn_TestStringArray(new String[]{"aaa","vvvv","cccccc"},3);
    // String []strs=pointer.getStringArray(0);
    // cLib.FilterKnn_ReleaseStringArray(pointer);
    // System.out.println(strs[0]+ "tttttt");
  }
}
