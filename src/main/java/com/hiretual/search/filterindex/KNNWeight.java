package com.hiretual.search.filterindex;

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */



//import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
//import com.amazon.opendistroforelasticsearch.knn.index.v2011.KNNIndex;
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.apache.lucene.index.FieldInfo;
// import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
// import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
// import org.apache.lucene.store.FSDirectory;
// import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.DocIdSetBuilder;
// import org.elasticsearch.common.io.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
// import java.nio.file.Path;
import java.util.*;
// import java.util.stream.Collectors;

/**
 * Calculate query weights and build query scorers.
 */
public class KNNWeight extends Weight {
//    private static Logger logger = LogManager.getLogger(KNNWeight.class);
    private static  Logger logger = LoggerFactory.getLogger(KNNWeight.class);
    private final KNNQuery knnQuery;
    private final float boost;
    // private  KNNQueryResult[] fakeResults;

//    public static KNNIndexCache knnIndexCache = KNNIndexCache.getInstance();

    public KNNWeight(KNNQuery query, float boost) {
        super(query);
        this.knnQuery = query;
        this.boost = boost;
        // this.fakeResults=Utils.genFakeResults(10000);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
        return Explanation.match(1.0f, "No Explanation");
    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {

        int topK=(int)(knnQuery.getK()*knnQuery.getRation());
        // final KNNQueryResult[] results=new KNNQueryResult[topK];
        long numSearchVector=10*topK;
        long numSearchCluster=(long) Math.ceil((double)numSearchVector/knnQuery.getClusterAverageVector()) ;
       
        long []resultIds=new long[topK];
        float[]resultDistances=new float[topK];
        long resultNum=CLib.INSTANCE.FilterKnn_IvfpqSearch(knnQuery.getQueryVector(), numSearchCluster,numSearchVector,2,0, topK, resultIds, resultDistances);
        if(resultNum==0){
            logger.warn("ivfpq search error or got empty result,msg:"+CLib.INSTANCE.FilterKnn_GetErrorMsg());
            return null;
        }
        /**
         * Scores represent the distance of the documents with respect to given query vector.
         * Lesser the score, the closer the document is to the query vector.
         * Since by default results are retrieved in the descending order of scores, to get the nearest
         * neighbors we are inverting the scores.
         */
        
        Map<Integer, Float> scores=new HashMap<>();
        // Map<Integer, Float> scores = Arrays.stream(resultIds).collect(
        //         Collectors.toMap(result -> result.getId(), result -> normalizeScore(result.getScore())));
        for(int i=0;i<resultNum;i++){
            scores.put((int)resultIds[i], normalizeScore(resultDistances[i]));
        }
        int maxDoc = Collections.max(scores.keySet()) + 1;
        DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
        DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);
        Arrays.stream(resultIds).forEach(result -> setAdder.add((int)result));
        DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
        logger.debug("scorer construct");
        return new KNNScorer(this, docIdSetIter, scores, boost);
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }

    public static float normalizeScore(float score) {
        if (score >= 0)
            return 1 / (1 + score);
        return -score + 1;
    }
}

