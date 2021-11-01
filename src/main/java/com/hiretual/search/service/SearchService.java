package com.hiretual.search.service;

import com.hiretual.search.utils.GlobalPropertyUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.apache.lucene.analysis.Analyzer;
// import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.*;
import org.springframework.stereotype.Service;
// import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import com.hiretual.search.filterindex.*;

@Service
public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger(SearchService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    private static final String SEARCH_THRESHOLD=GlobalPropertyUtils.get("search.threshold");
    // private static  Analyzer analyzer = new StandardAnalyzer();
    private static  IndexReader indexReader;
    private static IndexSearcher indexSearcher;
    private static int searchThreshold;

    private static CLib clib = CLib.INSTANCE;
    static {
        
        try {
            searchThreshold = Integer.parseInt(SEARCH_THRESHOLD);
        } catch(NumberFormatException e) {
            logger.warn("fail to initialize search threshold for index searcher");
        }
        try {
            logger.info(USER_HOME);
            indexReader=DirectoryReader.open(FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
            indexSearcher = new IndexSearcher(indexReader);
        } catch (IOException e) {
            logger.warn("fail to initialize index searcher");
        }
       
    }
     /**
     * search api combine ann and lucene filter
     * @param queryWrapper
     * @param size
     * @return
     * @throws IOException
     */
    public KNNQueryResult[] search(FakeQueryWrapper queryWrapper, int size) throws IOException {
        int totalDocNum=indexReader.maxDoc();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        List<Query> filterQuerys = queryWrapper.getFilterQuerys();
        for (int i = 0; i < filterQuerys.size(); i++) {
            builder.add(filterQuerys.get(i), BooleanClause.Occur.FILTER);
        }
        Query query = builder.build();
        //detection,a little trick ,set the numHits to threshold+1 ,maybe improve performance
        TopDocs filterTopDocs = indexSearcher.search(query, searchThreshold+1);
        long filterNum=filterTopDocs.totalHits.value;
        //if filter result num less then threshold,call the jni bruteforce function with filtered ids and query
        //and the result can be return directly as final result
        if (filterNum < searchThreshold) {
            ScoreDoc[]filterScoreDocs=filterTopDocs.scoreDocs;
            int flatSearchScale=filterScoreDocs.length;
            long []id=new long[filterScoreDocs.length];
            for(int i=0;i<flatSearchScale;i++){
                id[i]=filterScoreDocs[i].doc;
            }
            long[]resultIds=new long[size];
            float[]resultDistances=new float[size];
            long resultNum=clib.FilterKnn_FlatSearch(queryWrapper.getKnnQuery().getQueryVector(), id, flatSearchScale,19, size, resultIds, resultDistances);
            if(resultNum<=0){
                logger.warn("flat search error or got empty result");
                return null;
            }
            KNNQueryResult[] results=new KNNQueryResult[(int)resultNum];
            for(int i=0;i<resultNum;i++){
                KNNQueryResult knnQueryResult=new KNNQueryResult((int)resultIds[i], resultDistances[i]);
                results[i]=knnQueryResult;
            }
            return results;
        }
        //else if filter result num more then threshold,add knnQuery to builder ,then build a new BooleanQuery,
        //execute this query ，as a result ，we can got a result combine ann with lucene filter
        KNNQuery knnQuery=queryWrapper.getKnnQuery();
        knnQuery.setRation(totalDocNum/filterNum);
        builder.add(knnQuery, BooleanClause.Occur.MUST);
        query = builder.build();
        ScoreDoc[] hits = indexSearcher.search(query, size).scoreDocs;
        KNNQueryResult[] results = Utils.transformScoreDocToKNNQueryResult(hits);
        return results;

    }

}
