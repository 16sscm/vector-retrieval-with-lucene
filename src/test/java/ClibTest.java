import com.hiretual.search.filterindex.*;
import com.hiretual.search.model.FilterQuery;
import com.hiretual.search.model.FilterResume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.utils.RequestParser;
import com.sun.jna.Pointer;
import java.util.Random;
import org.junit.Test;

public class ClibTest {
    CLib cLib = CLib.INSTANCE;
    String USER_HOME = System.getProperty("user.home");
    String c_index_dir = USER_HOME + GlobalPropertyUtils.get("c_index_dir");

    String pFlatFile = c_index_dir + GlobalPropertyUtils.get("flat_file");
    String pIvfpqFile = c_index_dir + GlobalPropertyUtils.get("ivfpq_file");

    @Test
    public void testSearch() {
        int suc = cLib.FilterKnn_InitLibrary(128, 10000, 64, 8, "/root/FilterKnn_release/ivf_10000_pq_2_256.bin");
        double[] queryD = new double[] {
           -9.7531989e-02,  6.4633095e-01, -1.6879562e-01, -1.9502254e+00,
            7.3161721e-01,  6.4725596e-01, -1.8946533e-01,  1.7131418e-03,
            2.2293574e-01, -8.1100428e-01, -1.5417984e+00, -2.7902892e-01,
           -7.5131011e-01, -6.6540736e-01,  5.8186162e-01, -7.1481764e-01,
           -2.1194115e-01,  8.9249939e-01, -2.0290995e+00, -2.9499674e-01,
            7.6381725e-01, -9.7752053e-01,  3.8820970e-01,  5.4560310e-01,
            2.4354781e-01, -1.2706046e+00, -1.2178924e+00,  1.2420492e+00,
           -2.0206417e-01, -3.5922831e-01,  3.7215719e-01, -4.2393553e-01,
           -7.1626270e-01, -1.3955524e+00,  9.1343544e-02, -7.1836382e-01,
           -4.2355379e-01, -4.7355247e-01, -1.0566611e+00, -3.5999772e-01,
            1.8967375e-01,  7.2999418e-01,  3.7038764e-01, -2.6802945e-01,
           -3.8155246e-01,  1.1898045e-01, -8.9384019e-01,  8.1311029e-01,
           -7.3807377e-01, -7.3508359e-03,  1.2271395e+00,  1.2445321e+00,
           -1.2249240e+00,  1.3456039e-01,  5.7395464e-01, -1.2089888e+00,
            1.9328632e+00, -1.5855883e-01, -1.0393963e+00,  2.0466313e-01,
            1.7571623e+00, -1.5690435e+00, -8.1530774e-01,  2.5651474e-02,
           -4.6944374e-01,  1.0359195e+00, -3.2640696e-01,  6.5992844e-01,
            1.0823182e+00, -6.3461506e-01,  5.5736917e-01, -8.0373615e-02,
            2.2158539e-01, -1.5563092e+00, -9.7767162e-01, -6.6345441e-01,
            1.8047863e-01, -5.1301128e-01,  8.6964238e-01,  7.3665351e-01,
           -4.1791126e-02, -8.2656831e-02,  4.5525655e-01, -5.1144040e-01,
            8.8701159e-01,  4.2125756e-01, -1.4903519e-01, -7.6037347e-01,
            1.0081161e+00, -1.6956787e+00,  5.4521096e-01, -8.6615336e-01,
           -3.7138164e-04, -4.4535108e-02, -1.2533976e+00,  1.0290668e+00,
           -1.5121640e+00, -8.7159252e-01, -4.0245599e-01, -9.4496620e-01,
            7.8323966e-01,  1.1375573e+00, -1.7864015e+00,  7.6091051e-02,
           -3.8906842e-01, -3.3325890e-01,  4.4118401e-01, -4.6798944e-01,
            9.3959562e-02,  4.0016106e-01, -3.7264502e-01, -7.3845875e-01,
            1.3552098e+00, -5.8748400e-01, -1.1519476e+00, -6.1166257e-02,
           -6.1222130e-01, -7.8590167e-01, -8.0245972e-01, -8.1712711e-01,
            1.3818543e+00, -1.0459434e+00,  6.1577326e-01, -9.7827923e-01,
           -1.1692566e+00,  7.0240133e-02,  5.9796017e-01, -8.3342135e-01 };
        float[] query = new float[128];
        for (int i = 0; i < 128; i++) {
            query[i] = (float) queryD[i];
        }
        int topK = 1000;
        long resultIds1[] = new long[topK];
        float resultDistances1[] = new float[topK];

        System.out.println("ivfpq search...");
        long suc4 = cLib.FilterKnn_IvfpqSearch(query, 1000, 1000000, 100, 0, null, topK,
                resultIds1, resultDistances1);
        System.out.printf("ivfpq search result count: %d\n", suc4);

        if (suc4 > 0) {
            Pointer pointer = cLib.FilterKnn_GetUids(resultIds1, suc4);
            String[] uids = pointer.getStringArray(0);
            cLib.FilterKnn_ReleaseStringArray(pointer);
        }
    }

    public void testClib() {

        Random r = new Random();
        String msg = cLib.FilterKnn_GetErrorMsg();
        System.out.println(msg + "t");

        System.out.println("loading...");
        long t = System.currentTimeMillis();
        int suc = cLib.FilterKnn_InitLibrary(128, 10000, 64, 8, pIvfpqFile);
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
            id[i] = (long) i;
        }
        System.out.println("done!cost:" + (System.currentTimeMillis() - t));

        System.out.println("adding data...");
        t = System.currentTimeMillis();
        FilterResume filterResumes[] = new FilterResume[3];
        filterResumes[0] = new FilterResume("aaa");
        filterResumes[1] = new FilterResume("bbb");
        filterResumes[2] = new FilterResume("ccc");
        String str = RequestParser.getJsonString(filterResumes);
        int suc1 = cLib.FilterKnn_AddVectors(vectors, id, db, str);
        System.out.println("done!cost:" + (System.currentTimeMillis() - t));
        System.out.println(suc1 + "ttt");

        System.out.println("saving data...");
        t = System.currentTimeMillis();
        int suc2 = cLib.FilterKnn_Save(pIvfpqFile);
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
        String[] black_uids = new String[] { "aaa" };
        FilterQuery filterQuery = new FilterQuery(black_uids);
        String filterString = RequestParser.getJsonString(filterQuery);
        long suc3 = cLib.FilterKnn_FlatSearch(
                query, flatId, flat_db, 0, filterString, flatSearchSize, resultIds, resultDistances);
        System.out.println("done!cost:" + (System.currentTimeMillis() - t));
        System.out.println(suc3 + "ttttt");

        int topK = 1000;
        long resultIds1[] = new long[topK];
        float resultDistances1[] = new float[topK];
        System.out.println("ivfpq search...");
        t = System.currentTimeMillis();
        long suc4 = cLib.FilterKnn_IvfpqSearch(query, 1000, 10000, 5, 0, null, topK,
                resultIds1, resultDistances1);
        System.out.println("done!cost:" + (System.currentTimeMillis() - t));
        System.out.println(suc4 + "tttttt");
    }

    public void testPointer() {
        // CLib cLib = CLib.INSTANCE;
        // Pointer pointer=cLib.FilterKnn_TestStringArray(new
        // String[]{"aaa","vvvv","cccccc"},3);
        // String []strs=pointer.getStringArray(0);
        // cLib.FilterKnn_ReleaseStringArray(pointer);
        // System.out.println(strs[0]+ "tttttt");
    }
}
