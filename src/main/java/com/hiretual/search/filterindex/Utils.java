package com.hiretual.search.filterindex;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.util.*;

public class Utils {
    public static List<FakeDocument> genFakeDocuments(int size){
        Random r=new Random();
        String[]titles={"apache","lucene","index","apple","banana","orange","monday","Tuesday"};
        List<FakeDocument> list=new ArrayList<>();
        for(int i=0;i<size;i++){
            FakeDocument fakeDocument=new FakeDocument();
            fakeDocument.setId("_"+i);
            fakeDocument.setPrice(i);
            String title=titles[(int) ((titles.length-1)*r.nextFloat())];
            fakeDocument.setTitle(title);
            fakeDocument.setVector(getVector(128));
        }
        return list;
    }
    public static FakeQueryWrapper genFakeQuery(){
        FakeQueryWrapper fakeQuery=new FakeQueryWrapper();
        List<Query>filterQuerys=new ArrayList<>();
        filterQuerys.add(IntPoint.newRangeQuery("price", 0, 999999));
        fakeQuery.setKnnQuery(new KNNQuery("test_vec",getVector(128),10,"test_index"));
        fakeQuery.setFilterQuerys(filterQuerys);
        return fakeQuery;
    }
    public static KNNQueryResult[] transformScoreDocToKNNQueryResult(ScoreDoc[] hits){

        KNNQueryResult[] results=new KNNQueryResult[hits.length];
        for(int i=0;i<hits.length;i++){
            results[i]=new KNNQueryResult(hits[i].doc,hits[i].score);
        }
        return results;
    }
    public static Map<String,float[]>getRawMap(){
        Map<String,float[]> map=new HashMap<>();
        for(int i=0;i<10000;i++){
            map.put("_"+i,getVector(128));
        }

        return map;
    }
    public static   KNNQueryResult[]  genFakeResults(int size){
        Random random=new Random();
        KNNQueryResult[] fakeResults=new KNNQueryResult[size];
        for(int i=0;i<size;i++){
            fakeResults[i]=new KNNQueryResult(i,random.nextFloat());
        }
        return fakeResults;
    }
    private static float[] getVector(int dim){
        Random random = new Random();
        float[] vector=new float[dim];
        for(int j=0;j<dim;j++){
            vector[j]=random.nextFloat();
        }
        return vector;
    }
}

