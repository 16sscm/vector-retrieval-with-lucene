package com.hiretual.search.web;

import com.hiretual.search.filterindex.KNNQuery;
import com.hiretual.search.filterindex.KNNResult;
import com.hiretual.search.model.Resume;
import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.service.SearchService;
import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.utils.JedisUtils;
import com.hiretual.search.utils.QueryConvertor;
import com.hiretual.search.utils.RequestParser;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

@Component
@RestController
public class SearchController {
    private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

    @Autowired
    IndexBuildService indexBuildService;

    @Autowired
    SearchService searchService;

    @RequestMapping(value="/health/check", method={RequestMethod.GET, RequestMethod.POST})
    public String healthCheck() {
        return "ok\n";
    }

    @RequestMapping(value="/doc/add", method=RequestMethod.POST, produces="application/json;charset=UTF-8")
    public String insertDocument(HttpServletRequest request) {
        try{
            JsonNode doc = RequestParser.getPostParameter(request);
            Resume resume = new Resume(doc);
            indexBuildService.addDocument(resume);
           
        }catch(Exception e){
            logger.warn("fail to add document",e);
            return "-1";

        }
        return "0";
       
    }
   
    @RequestMapping(value="/index/merge", method={RequestMethod.GET, RequestMethod.POST})
    public void mergeIndex() {
        indexBuildService.mergeSegments();
    }
    @RequestMapping(value="/index/commit", method={RequestMethod.GET})
    public int commitAndCheckIndexSize() {
        return indexBuildService.commitAndCheckIndexSize();
    }
    @RequestMapping(value="/search", method=RequestMethod.POST)
    public String search(@RequestBody JsonNode query) {
        long t1 = System.currentTimeMillis();
        JsonNode embeddingJson=query.get("embedding");
        List<Double>embedding=RequestParser.transformJson2Array(embeddingJson.asText());
        JsonNode esQuery = QueryConvertor.extractESQuery(query);
        
        long t2 = System.currentTimeMillis();
        List<Query> queries = QueryConvertor.convertESQuery(esQuery);
        long t3 = System.currentTimeMillis();
        if(queries==null){
            return null;
        }
       
       
        int k = 1000;
       
        float[] v = new float[128];
        for (int i = 0; i < 128; i++) {
            v[i] = embedding.get(i).floatValue();
        }
        KNNQuery kq = new KNNQuery( v);

       
        String ret="";
        try {
            KNNResult[] result = searchService.search(queries, kq, k);
            
            if(result==null){
                logger.info("0 results");
                ret= "0";
                long end = System.currentTimeMillis();
                logger.info("total cost: " + (end - t3) + "|" + (t3-t2) +"|" +(t2-t1) + " ms");
            }else{
                logger.info(result.length + " results");
                ret=RequestParser.getJsonString(result);
                long end = System.currentTimeMillis();
                logger.info("total cost: " + (end - t3) + "|" + (t3-t2) +"|" +(t2-t1) + " ms");
            }
            
        } catch(Exception e) {
            ret="-1";
            logger.error("search error", e);
        }
       
        return ret;
    }
}
