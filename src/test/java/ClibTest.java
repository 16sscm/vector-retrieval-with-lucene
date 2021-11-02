import java.io.File;
import java.util.Random;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.filterindex.*;
import org.junit.Test;

public class ClibTest {
    @Test
    public void testClib(){
        CLib cLib=CLib.INSTANCE;
        String msg=cLib.FilterKnn_GetErrorMsg();
        System.out.println(msg+"tt");
        int suc=cLib.FilterKnn_InitLibrary(128,10000,64,8,"","");
        System.out.println(suc+"tt");
        float []vectors=new float[1280];
        long []id=new long[10];
        long n=10;
        Random r=new Random();
        for(int i=0;i<10;i++){
            for(int j=0;j<128;j++){
                vectors[i*128+j]=r.nextFloat();
            }
            id[i]=(long)i;
            
        }
        int suc1=cLib.FilterKnn_AddVectors(vectors, id, n);
        System.out.println(suc1+"tt");
        String USER_HOME = System.getProperty("user.home");
       
        String c_index=USER_HOME+ GlobalPropertyUtils.get("c_index");
        File file=new File(c_index);
		if(!file.exists()){//如果文件夹不存在
			file.mkdir();//创建文件夹
		}
        String pFlatFile=c_index+"/pFlatFile";
        String pIvfpqFile=c_index+"/pIvfpqFile";
        int suc2=cLib.FilterKnn_Save(pFlatFile, pIvfpqFile);
        System.out.println(suc2+"tt");
        long resultIds[]=new long[3];
        float resultDistances[]=new float[3];
        long suc3=cLib.FilterKnn_FlatSearch(vectors, id, 10,19, 3, resultIds, resultDistances);
        System.out.println(suc3+"tt");
    }
}
