package com.hiretual.search.filterindex;
import org.apache.lucene.search.Query;

import java.util.List;

public class FakeQueryWrapper {
    KNNQuery knnQuery;
    List<Query> filterQuerys;

    public List<Query> getFilterQuerys() {
        return filterQuerys;
    }

    public void setFilterQuerys(List<Query> filterQuerys) {
        this.filterQuerys = filterQuerys;
    }

    public KNNQuery getKnnQuery() {
        return knnQuery;
    }

    public void setKnnQuery(KNNQuery knnQuery) {
        this.knnQuery = knnQuery;
    }
}
