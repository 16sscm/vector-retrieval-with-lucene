import java.io.File;
import java.util.Random;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.filterindex.*;
import org.junit.Test;

public class ClibTest {
    @Test
    public void testClib(){
        CLib cLib=CLib.INSTANCE;
        Random r=new Random();
        String msg=cLib.FilterKnn_GetErrorMsg();
        System.out.println(msg+"t");

        System.out.println("loading...");
        long t=System.currentTimeMillis();
        int suc=cLib.FilterKnn_InitLibrary(128,100000,64,8,"/root/c_index/pFlatFile","/root/c_index/ivfpq.bin");
        System.out.println("done!cost:"+(System.currentTimeMillis()-t));
        System.out.println(suc+"tt");

        // int db=10000000;
        // float []vectors=new float[128*db];
        // long []id=new long[db];
        // // long n=10;
        
        // System.out.println("gendata...");
        // t=System.currentTimeMillis();
        // for(int i=0;i<db;i++){
        //     for(int j=0;j<128;j++){
        //         vectors[i*128+j]=r.nextFloat();
        //     }
        //     id[i]=(long)i;
            
        // }
        // System.out.println("done!cost:"+(System.currentTimeMillis()-t));

        // System.out.println("adding data...");
        // t=System.currentTimeMillis();
        // int suc1=cLib.FilterKnn_AddVectors(vectors, id, db);
        // System.out.println("done!cost:"+(System.currentTimeMillis()-t));
        // System.out.println(suc1+"ttt");

        // String USER_HOME = System.getProperty("user.home");
        // String c_index=USER_HOME+ GlobalPropertyUtils.get("c_index_dir");
        // File file=new File(c_index);
		// if(!file.exists()){//如果文件夹不存在
		// 	file.mkdir();//创建文件夹
		// }
        // String pFlatFile=c_index+"/pFlatFile";
        // String pIvfpqFile=c_index+"/ivfpq.bin";
        // System.out.println("saving data...");
        // t=System.currentTimeMillis();
        // int suc2=cLib.FilterKnn_Save(pFlatFile, pIvfpqFile);
        // System.out.println("done!cost:"+(System.currentTimeMillis()-t));
        // System.out.println(suc2+"tttt");

        float []query=new float[128];
        for(int i=0;i<128;i++){
            query[i]=r.nextFloat();
        }
        int flat_db=1000000;
        long []flatId=new long[flat_db];
        for(int i=0;i<flat_db;i++){
            flatId[i]=i;
        }
        int flatSearchSize=1000;
        long resultIds[]=new long[flatSearchSize];
        float resultDistances[]=new float[flatSearchSize];
        System.out.println("flat search...");
        t=System.currentTimeMillis();
        long suc3=cLib.FilterKnn_FlatSearch(query, flatId, flat_db,0, flatSearchSize, resultIds, resultDistances);
        System.out.println("done!cost:"+(System.currentTimeMillis()-t));
        System.out.println(suc3+"ttttt");


        int topK=100000;
        long resultIds1[]=new long[topK];
        float resultDistances1[]=new float[topK];
        System.out.println("ivfpq search...");
        t=System.currentTimeMillis();
        long suc4=cLib.FilterKnn_IvfpqSearch(query, 5000,500000,0, topK, resultIds1, resultDistances1);
        System.out.println("done!cost:"+(System.currentTimeMillis()-t));
        System.out.println(suc4+"tttttt");
    }
}
