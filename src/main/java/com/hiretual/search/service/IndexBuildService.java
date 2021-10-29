package com.hiretual.search.service;

import com.hiretual.search.model.Resume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import com.hiretual.search.filterindex.*;

@Service
public class IndexBuildService {

    private static final Logger logger = LoggerFactory.getLogger(IndexBuildService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    private static final String MAX_MEMORY = GlobalPropertyUtils.get("max.memory");

    private static Analyzer analyzer = new StandardAnalyzer();
    private static  IndexReader indexReader;
    private static IndexSearcher indexSearcher;
    private static IndexWriter writer;
    private static int maxMemory;
    private static FieldType textFieldType = new FieldType(TextField.TYPE_NOT_STORED);

    private static CLib clib = CLib.INSTANCE;
    static {
        textFieldType.setStoreTermVectors(true);
        textFieldType.setStoreTermVectorPositions(true);
        textFieldType.setStoreTermVectorOffsets(true);
        textFieldType.setStoreTermVectorPayloads(false);
        try {
            maxMemory = Integer.parseInt(MAX_MEMORY);
        } catch(NumberFormatException e) {
            logger.warn("fail to initialize max memory for index writer");
        }
        try {
            logger.info(USER_HOME);
            Directory dir = FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            iwc.setRAMBufferSizeMB(maxMemory);
            indexReader=DirectoryReader.open(FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
            indexSearcher = new IndexSearcher(indexReader);
            writer = new IndexWriter(dir, iwc);
        } catch (IOException e) {
            logger.warn("fail to initialize index writer");
        }
    }

    public boolean addDocument(List<Resume> resumes) {
        

        //TODO: to be implemented
        try {


            writer.commit();
             //It seems necessary to merge the segments
            writer.forceMerge(1);
            writer.close();
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }
     /**
     * read ids from lucene by docid,get the corresponding vector
     * add and save to both ann and bruteforce index
     */
    public void annBruteforceIndex(int dim,String pFlatFile,String pIvfpqFile) throws IOException {
       
        int totalDocNum=indexReader.maxDoc();
        int ids[]=new int[totalDocNum];
        float[][]vectors=new float[totalDocNum][dim];
        for(int i=0;i<totalDocNum;i++){
            Document document=indexSearcher.doc(i);
            String id=document.get("id");
            float[]vector=Utils.getRawMap().get(id);
            ids[i]=i;
            vectors[i]=vector;
        }
        clib.FilterKnn_AddVectors(vectors,ids,ids.length);
        clib.FilterKnn_Save(pFlatFile,pIvfpqFile);
       
    }
}
