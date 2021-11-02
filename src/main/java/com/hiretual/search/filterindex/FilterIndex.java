package com.hiretual.search.filterindex;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class FilterIndex {
    Analyzer analyzer;
    Directory dir;
    IndexWriterConfig iwc;
    int threshold;
    CLib clib = CLib.INSTANCE;

    FilterIndex(String directory, int threshold) throws IOException {
        this.analyzer = new StandardAnalyzer(); // 标准分词器
        this.iwc = new IndexWriterConfig(analyzer);
        this.threshold = threshold;
        // 1. create the index
        this.dir = MMapDirectory.open(Paths.get(directory));
        // clib.FilterKnn_InitLibrary();
        
    }

    /**
     * lucene index
     * @param documentList
     * @throws IOException
     */
    public void index(List<FakeDocument> documentList) throws IOException {
        int size = documentList.size();
        IndexWriter writer = new IndexWriter(dir, iwc);
        for (int i = 0; i < size; i++) {
            FakeDocument fakeDocument = documentList.get(i);
            // float[] vec = fakeDocument.getVector();
            Document knnDocument = new Document();
            // knnDocument.add(
            //         new BinaryDocValuesField(
            //                 "vector",
            //                 new VectorField("vector", vec, new FieldType()).binaryValue()));
//                knnDocument.add(new SortedDocValuesField("_id",new StringField("_id","_",new Store()).binaryValue()));

            knnDocument.add(new StoredField("id", fakeDocument.getId()));
//                int price=(int)(100*r.nextFloat());
            // int price = i;
//                System.out.println(price);
            knnDocument.add(new LongPoint("price", fakeDocument.getPrice()));
            knnDocument.add(new TextField("title", fakeDocument.getTitle(), Field.Store.YES));
            writer.addDocument(knnDocument);
        }
        writer.commit();
//            w.optimize();
        //It seems necessary to merge the segments
        writer.forceMerge(1);
        writer.close();

    }

    /**
     * search api combine ann and lucene filter
     * @param queryWrapper
     * @param size
     * @return
     * @throws IOException
     */
    public KNNQueryResult[] search(FakeQueryWrapper queryWrapper, int size) throws IOException {
        IndexReader reader = DirectoryReader.open(dir);
        int totalDocNum=reader.maxDoc();
        IndexSearcher searcher = new IndexSearcher(reader);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        List<Query> filterQuerys = queryWrapper.getFilterQuerys();
        for (int i = 0; i < filterQuerys.size(); i++) {
            builder.add(filterQuerys.get(i), BooleanClause.Occur.FILTER);
        }
        Query query = builder.build();
        //detection,a little trick ,set the numHits to threshold+1 ,maybe improve performance
        TopDocs filterTopDocs = searcher.search(query, threshold+1);
        //if filter result num less then threshold,call the  jni bruteforce function with filter ids and query
        //and the result can be return directly
        long filterNum=filterTopDocs.totalHits.value;
        if (filterNum < threshold) {
            // ScoreDoc[]filterScoreDocs=filterTopDocs.scoreDocs;
            // int []ids=new int[filterScoreDocs.length];
            // return clib.bruteforce_search(ids,queryWrapper.knnQuery.getQueryVector(), size);
        }
        //else if filter result num more then threshold,add knnQuery to builder ,then build a new BooleanQuery,
        //execute this query ，as a result ，we can got a result combine ann with lucene filter
        KNNQuery knnQuery=queryWrapper.getKnnQuery();
        knnQuery.setRation(totalDocNum/filterNum);
        builder.add(knnQuery, BooleanClause.Occur.MUST);
        query = builder.build();
        ScoreDoc[] hits = searcher.search(query, size).scoreDocs;
        KNNQueryResult[] results = Utils.transformScoreDocToKNNQueryResult(hits);
        return results;

    }
    /**
     * read ids from lucene by docid,get the corresponding vector
     * add and save to both ann and bruteforce index
     */
    public void annBruteforceIndex(int dim,String path) throws IOException {
        IndexReader reader = DirectoryReader.open(dir);
        int totalDocNum=reader.maxDoc();
        IndexSearcher searcher = new IndexSearcher(reader);
        int ids[]=new int[totalDocNum];
        float[][]vectors=new float[totalDocNum][dim];
        for(int i=0;i<totalDocNum;i++){
            Document document=searcher.doc(i);
            String id=document.get("id");
            float[]vector=Utils.getRawMap().get(id);
            ids[i]=i;
            vectors[i]=vector;
        }
        // clib.bruteforce_add(ids,vectors);
        // clib.bruteforce_save(path);
        // clib.ann_add();
        // clib.ann_save(path);
    }
}

