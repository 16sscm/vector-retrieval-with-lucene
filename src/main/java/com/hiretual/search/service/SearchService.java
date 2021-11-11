package com.hiretual.search.service;

import com.hiretual.search.utils.GlobalPropertyUtils;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.apache.lucene.analysis.Analyzer;
// import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.*;
import org.springframework.stereotype.Service;
// import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
            indexReader=DirectoryReader.open(MMapDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
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
    public KNNResult[] search(List<Query> querys, KNNQuery kq, int size) throws IOException {
        long t1 = System.currentTimeMillis();
        int totalDocNum=indexReader.maxDoc();
        int numDocs=indexReader.numDocs();
        if(totalDocNum!=numDocs){
          logger.warn("stop search,invalid document num for index,numDocs: "+numDocs+",maxDoc:"+totalDocNum);
          return null;
        }
        Query query;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        
        for(Query q:querys){
            logger.info(indexSearcher.count(q) +"|"+q.toString());
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        query=builder.build();
        logger.info(query.toString());
        //detection
        
        int count=indexSearcher.count(query);
        long t2 = System.currentTimeMillis();
        //if filter result num less then threshold,call the jni bruteforce function with filtered ids and query
        //and the result can be return directly as final result
        
        if (count < searchThreshold) {
            if(count==0){
                return null;
            }
            TopDocs filterTopDocs = indexSearcher.search(query, count);//as much as possible
            long t3 = System.currentTimeMillis();
            ScoreDoc[]filterScoreDocs=filterTopDocs.scoreDocs;
            int flatSearchScale=filterScoreDocs.length;
            long []id=new long[flatSearchScale];
            for(int i=0;i<flatSearchScale;i++){
                id[i]=filterScoreDocs[i].doc;
            }
            long[]resultIds=new long[size];
            float[]resultDistances=new float[size];
            long t4 = System.currentTimeMillis();
            long resultNum=clib.FilterKnn_FlatSearch(kq.getQueryVector(), id, flatSearchScale,0, size, resultIds, resultDistances);
            long t5 = System.currentTimeMillis();
            if(resultNum<=0){
                logger.warn("flat search error or got empty result,msg:"+clib.FilterKnn_GetErrorMsg());
                return null;
            }
            // KNNQueryResult[] results=new KNNQueryResult[(int)resultNum];
            // for(int i=0;i<resultNum;i++){
            //     KNNQueryResult knnQueryResult=new KNNQueryResult((int)resultIds[i], KNNWeight.normalizeScore( resultDistances[i]));
            //     results[i]=knnQueryResult;
            // }
            logger.info("" + (t2-t1) +"|"+ (t3-t2) +"|"+ (t4-t3) +"|"+ (t5-t4));
            return convertFlatResult2KNNResult(resultNum,resultIds,resultDistances);
        }
        //else if filter result num more then threshold,add knnQuery to builder ,then build a new BooleanQuery,
        //execute this query ，as a result ，we can got a result combine ann with lucene filter
       
        kq.setRation(totalDocNum/count);
        builder.add(kq, BooleanClause.Occur.MUST);
        query = builder.build();
        ScoreDoc[] hits = indexSearcher.search(query, size).scoreDocs;
        // KNNQueryResult[] results = Utils.transformScoreDocToKNNQueryResult(hits);
        return convertScoreDoc2KNNResult(hits);

    }
   
    private KNNResult[] convertFlatResult2KNNResult(long size,long[]resultIds,float[]resultDistances){
       
        try{
            KNNResult[] results=new KNNResult[(int)size];
            HashSet<String> uidField = new HashSet<>();
            uidField.add("uid");
            for(int i=0;i<size;i++){
                Document doc =indexReader.document((int)resultIds[i],uidField);
                String uid = doc.get("uid");
                results[i]=new KNNResult(uid, KNNWeight.normalizeScore( resultDistances[i]));
                
            }
            return results;
        }catch(IOException e){
            e.printStackTrace();
           
        }
        return null;
       
    }
    private  KNNResult[] convertScoreDoc2KNNResult(ScoreDoc[] scoreDocs){
       
        try{
            KNNResult[] results=new KNNResult[scoreDocs.length];
            HashSet<String> uidField = new HashSet<>();
            uidField.add("uid");
            for(int i=0;i<scoreDocs.length;i++){
                Document doc =indexReader.document(scoreDocs[i].doc,uidField);
                String uid = doc.get("uid");
                results[i]=new KNNResult(uid, scoreDocs[i].score);
            }
            return results;
        }catch(IOException e){
            e.printStackTrace();
           
        }
        return null;
       
    }

}
