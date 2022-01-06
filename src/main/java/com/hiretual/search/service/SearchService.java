package com.hiretual.search.service;

import com.hiretual.search.utils.GlobalPropertyUtils;
import com.sun.jna.Pointer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
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
import java.util.Random;

import com.hiretual.search.filterindex.*;

@Service
public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger(SearchService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    private static final String SEARCH_THRESHOLD=GlobalPropertyUtils.get("search.threshold");
    private static String mode = GlobalPropertyUtils.get("mode");
    // private static  Analyzer analyzer = new StandardAnalyzer();
    protected static  DirectoryReader indexReader;
    protected static IndexSearcher indexSearcher;
    private static int searchThreshold;
    private static int numDocs;
    private static CLib clib = CLib.INSTANCE;
    static {
        
        try {
            searchThreshold = Integer.parseInt(SEARCH_THRESHOLD);
        } catch(NumberFormatException e) {
            logger.warn("fail to initialize search threshold for index searcher");
        }
        if(mode.equals("search")){
            lazyInit();
            logger.info("search mode,open search directly");
        }
       
    }
    /**
     * search will change with writer change
     * @param writer
     */
    public static void lazyInit(){
        try {
            if(indexReader==null){
                logger.info("init searcher");
               
            }else{
                logger.info("re-open searcher");
                
            }
            indexReader= DirectoryReader.open(
                FSDirectory.open(Paths.get(INDEX_FOLDER)));
            indexSearcher = new IndexSearcher(indexReader);
            int maxDocNum=indexReader.maxDoc();
            numDocs=indexReader.numDocs();
            if(maxDocNum!=numDocs){
            logger.warn("search service init failed,invalid document num for index,numDocs: "+numDocs+",maxDoc:"+maxDocNum);
           
            }
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
        
        Query query;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        
        for(Query q:querys){
            // logger.info(indexSearcher.count(q) +"|"+q.toString());
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
            long resultNum=clib.FilterKnn_FlatSearch(kq.getQueryVector(), id, flatSearchScale,0,null,size, resultIds, resultDistances);
            long t5 = System.currentTimeMillis();
            if(resultNum<=0){
                logger.warn("flat search error or got empty result,msg:"+clib.FilterKnn_GetErrorMsg());
                return null;
            }
            logger.info("count:"+count+"," + (t2-t1) +"|"+ (t3-t2) +"|"+ (t4-t3) +"|"+ (t5-t4));
            return convertFlatResult2KNNResult(resultNum,resultIds,resultDistances);
        }
        //else if filter result num more then threshold,add knnQuery to builder ,then build a new BooleanQuery,
        //execute this query ，as a result ，we can got a result combine ann with lucene filter
       
        kq.setK(size);
        kq.setClusterAverageVector(numDocs/IndexBuildService.numIvfCluster);
        kq.setRation((float)numDocs/count);
        builder.add(kq, BooleanClause.Occur.MUST);
        query = builder.build();
         
        Query query1;
        BooleanQuery.Builder builder1 = new BooleanQuery.Builder();
        builder1.add(kq, BooleanClause.Occur.MUST);
        for(Query q:querys){
            // logger.info(indexSearcher.count(q) +"|"+q.toString());
            builder1.add(q, BooleanClause.Occur.FILTER);
        }
        query1=builder1.build();

        ScoreDoc[] hits = indexSearcher.search(query1, size).scoreDocs;
        long t3 = System.currentTimeMillis();
        logger.info("count:"+count+"," + (t2-t1) +"|"+ (t3-t2));
        // KNNQueryResult[] results = Utils.transformScoreDocToKNNQueryResult(hits);
        return convertScoreDoc2KNNResult(hits);

    }
   
    private KNNResult[] convertFlatResult2KNNResult(long size,long[]resultIds,float[]resultDistances){
        long t1 = System.currentTimeMillis();
        KNNResult[] results=new KNNResult[(int)size];
        Pointer pointer=clib.FilterKnn_GetUids(resultIds,size);
        String []uids=pointer.getStringArray(0);
        clib.FilterKnn_ReleaseStringArray(pointer);
        for(int i=0;i<size;i++){
            
            String rawJson="";
            try {
                Document doc = indexReader.document((int)resultIds[i]);
                rawJson=doc.get("raw_json");
            } catch (IOException e) {
                logger.warn("fail to get raw json from lucene",e);
            }
            
            results[i]=new KNNResult(uids[i], KNNWeight.normalizeScore(resultDistances[i]),rawJson);
        }
        long t2 = System.currentTimeMillis();
        logger.info("convertFlatResult2KNNResult cost: "+(t2-t1));
        return results;
    }
    private  KNNResult[] convertScoreDoc2KNNResult(ScoreDoc[] scoreDocs){
       
        
        long t1 = System.currentTimeMillis();
        int size=scoreDocs.length;
        KNNResult[] results=new KNNResult[size];
        long []ids=new long[size];
        for(int i=0;i<size;i++){
            ids[i]=scoreDocs[i].doc;
        }
        Pointer pointer=clib.FilterKnn_GetUids(ids,size);
        String []uids=pointer.getStringArray(0);
        clib.FilterKnn_ReleaseStringArray(pointer);
       
        for(int i=0;i<size;i++){
            
            String rawJson="";
            try {
                Document doc = indexReader.document((int)ids[i]);
                rawJson=doc.get("raw_json");
            } catch (IOException e) {
                logger.warn("fail to get raw json from lucene",e);
            }
            
            results[i]=new KNNResult(uids[i], scoreDocs[i].score,rawJson);
        }
        long t2 = System.currentTimeMillis();
        logger.info("convertScoreDoc2KNNResult cost: "+(t2-t1));
        return results;
        
       
    }


}
