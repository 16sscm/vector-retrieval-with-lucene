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
        int suc = cLib.FilterKnn_InitLibrary(128, 10000, 2, 256, "/root/ivf_10000_pq_2_256.bin");
        double[] queryD = new double[] {
            1.8178300e+00,  1.3341966e+00,  1.4686838e+00,  2.2286918e+00,
        1.9853102e+00,  1.0299859e+00,  7.5971550e-01, -2.2765882e+00,
       -2.1735430e+00, -1.3087360e+00, -9.4944441e-01, -1.4887123e+00,
        1.7056503e+00, -2.1136584e+00,  2.2334358e-01,  1.1607292e+00,
        1.2300905e+00,  2.1198928e+00,  6.5247798e-01,  3.5752985e+00,
        6.8711609e-01, -1.5701564e-01,  5.9558749e-01, -2.1828927e-02,
        2.1361456e+00,  4.5286274e-01,  2.2305636e-01,  6.2968865e-02,
       -4.6453732e-01, -7.4076259e-01, -1.2440857e+00, -4.4418287e-01,
       -1.3760082e+00, -3.2740641e+00,  5.5000354e-02, -1.5911788e+00,
        5.2067274e-01, -2.7092535e+00, -1.8128145e+00, -3.0070551e-02,
       -6.5127976e-02,  1.7126718e+00,  1.4708898e+00,  1.3683928e+00,
       -1.5748795e+00,  2.7918562e-01, -5.9031081e-01, -2.5185418e+00,
       -2.1031969e+00, -3.8459527e-01,  8.9097333e-01,  1.4191431e+00,
       -1.7883279e+00, -6.9871271e-01, -2.7609413e-02, -9.3988299e-01,
        1.2008009e+00,  1.2863439e+00, -1.1547985e+00, -2.0603809e+00,
       -4.9395883e-01,  2.3969145e-01, -1.5390805e+00, -5.8473104e-01,
        1.6540625e+00,  1.9477360e-02, -4.5466477e-01,  2.4091406e+00,
        1.4723120e+00,  6.8105504e-02,  2.5639768e+00,  2.8442140e+00,
        1.3021734e+00,  8.8524640e-01, -1.0971527e+00,  3.0246952e-01,
        7.7300048e-01, -1.4066528e+00, -3.9297041e-01, -5.4343665e-01,
       -1.8723748e+00, -1.2456648e+00,  5.3890353e-01, -2.4805188e+00,
        4.5042518e-01, -1.0634637e+00, -1.7779623e+00, -3.3380786e-01,
       -1.8376548e+00, -1.4754016e+00,  2.7002330e+00,  2.7414250e-01,
       -1.3061653e+00, -1.3435003e-01,  2.1821482e-01,  1.6198257e+00,
       -1.7672123e+00, -1.1632237e+00, -2.6228752e+00,  1.3818129e+00,
        1.6680079e+00, -1.7342414e+00,  2.6547882e-01,  1.3988497e+00,
       -1.3822678e+00, -1.3659158e+00,  6.5590584e-01, -2.9734678e+00,
        2.3739551e-01, -2.2781887e+00, -1.5412936e+00,  5.8295161e-02,
       -6.6155267e-01, -2.0511799e-02,  6.4275318e-01,  1.2756014e+00,
       -2.3103878e-03, -2.6734288e+00, -4.4669101e-01, -2.3042710e+00,
        2.4039624e+00,  9.4201732e-01, -1.0925876e+00, -2.1878436e+00,
        8.1929898e-01,  2.4280775e+00, -4.4634956e-01, -3.4809458e-01};
        float[] query = new float[128];
        for (int i = 0; i < 128; i++) {
            query[i] = (float) queryD[i];
        }
        int topK = 1000;
        long resultIds1[] = new long[topK];
        float resultDistances1[] = new float[topK];

        System.out.println("ivfpq search...");
        long suc4 = cLib.FilterKnn_IvfpqSearch(query, 1000, 1000000, 1,20, 0, null, topK,
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
        int suc = cLib.FilterKnn_InitLibrary(128, 10000, 2, 256, pIvfpqFile);
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
        long suc4 = cLib.FilterKnn_IvfpqSearch(query, 1000, 10000, 1,20, 0, null, topK,
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
