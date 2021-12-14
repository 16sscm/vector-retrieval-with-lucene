import com.hiretual.search.model.DistributeInfo;
import com.hiretual.search.service.IndexBuildService;

import org.junit.Test;

public class IndexBuildTest {
    @Test
    public void testProcess(){
        DistributeInfo info=new DistributeInfo(4,0);

        IndexBuildService indexBuildService=new IndexBuildService();
        indexBuildService.process(info);
    }
}
