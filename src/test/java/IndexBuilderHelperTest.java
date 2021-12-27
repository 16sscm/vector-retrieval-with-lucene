import com.hiretual.search.model.DistributeInfo;
import com.hiretual.search.service.IndexBuilderHelper;

import org.junit.Test;
import org.junit.rules.DisableOnDebug;

public class IndexBuilderHelperTest {
    // @Test
    public void testGetFileList(){
        DistributeInfo info=new DistributeInfo(4,3);
        IndexBuilderHelper helper=new IndexBuilderHelper(info);
        helper.getRawJsonList();

    }
}
