package com.hiretual.search.service;

import com.hiretual.search.filterindex.*;
import com.hiretual.search.model.Resume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.utils.JedisUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class IndexBuildService {

  private static final Logger logger =
      LoggerFactory.getLogger(IndexBuildService.class);

  private static final String USER_HOME = System.getProperty("user.home");
  private static final String INDEX_FOLDER =
      GlobalPropertyUtils.get("index.folder");
  private static final String MAX_MEMORY =
      GlobalPropertyUtils.get("max.memory");
  private static final String EMBEDDING_DIMENSION =
      GlobalPropertyUtils.get("embedding.dimension");
  private static String NUM_IVF_CLUSTER=GlobalPropertyUtils.get("num_ivf_cluster");
  
  @Autowired
  private JedisUtils jedisUtils;

  private static Analyzer analyzer = new StandardAnalyzer();
  private static IndexWriter writer;
  private static int maxMemory;
  // private static Map<String, float[]> uidEmbeddingMap = new HashMap<>();
  public static int embeddingDimension;
  static String c_index_dir =
      USER_HOME + GlobalPropertyUtils.get("c_index_dir");

  static String pFlatFile = c_index_dir + GlobalPropertyUtils.get("flat_file");
  static String pIvfpqFile = c_index_dir + GlobalPropertyUtils.get("ivfpq_file");
  public static int numIvfCluster;
  private static CLib clib = CLib.INSTANCE;

  static {
    File file = new File(c_index_dir);
    if (!file.exists()) { 
      file.mkdir();    
    }
    try {
      maxMemory = Integer.parseInt(MAX_MEMORY);
      embeddingDimension = Integer.parseInt(EMBEDDING_DIMENSION);
      numIvfCluster=Integer.parseInt(NUM_IVF_CLUSTER);
    } catch (NumberFormatException e) {
      logger.warn("fail to initialize max memory for index writer");
    }
    try {
      logger.info(USER_HOME);
      Directory dir = FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER));
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      iwc.setRAMBufferSizeMB(maxMemory);
      writer = new IndexWriter(dir, iwc);
    } catch (IOException e) {
      logger.warn("fail to initialize index writer");
    }
    // load knn model(trained) and flat map if exist to make it  searchable,
    // otherwise you should  add the vector first,and remember to save
    int suc=clib.FilterKnn_InitLibrary(embeddingDimension, numIvfCluster, 64, 8, pFlatFile, pIvfpqFile);
    if(suc==0){
      logger.error("fail to initialize clib,msg:"+clib.FilterKnn_GetErrorMsg());
    }
  }

  public void addDocument(List<Resume> resumes) {
    int count = 0;
    for (Resume resume : resumes) {
      Document doc = new Document();

      Field uidField = new StringField("uid", resume.getUid(), Field.Store.YES);
      doc.add(uidField);

      Field hotField4Sort = new NumericDocValuesField(
          "score", resume.isHasPersonalEmail() ? resume.getAvailability() + 1000
                                               : resume.getAvailability());
      doc.add(hotField4Sort);

      if (!StringUtils.isEmpty(resume.getDegree())) {
        Field degreeField =
            new StringField("degree", resume.getDegree(), Field.Store.NO);
        doc.add(degreeField);
      }

      if (!StringUtils.isEmpty(resume.getYoe())) {
        Field yoeField =
            new StringField("yoe", resume.getYoe(), Field.Store.NO);
        doc.add(yoeField);
      }

      if (!StringUtils.isEmpty(resume.getMonthsCurrentCompany())) {
        Field mccField = new IntPoint("mcc", resume.getMonthsCurrentCompany());
        doc.add(mccField);
      }

      if (!StringUtils.isEmpty(resume.getMonthsCurrentRole())) {
        Field mcrField = new IntPoint("mcr", resume.getMonthsCurrentRole());
        doc.add(mcrField);
      }

      Field divWomanField =
          new IntPoint("divWoman", resume.isDivWoman() ? 1 : 0);
      doc.add(divWomanField);

      Field divBlackField =
          new IntPoint("divBlack", resume.isDivBlack() ? 1 : 0);
      doc.add(divBlackField);

      Field divHispanicField =
          new IntPoint("divHispanic", resume.isDivHispanic() ? 1 : 0);
      doc.add(divHispanicField);

      Field divVeteranField =
          new IntPoint("divVeteran", resume.isDivVeteran() ? 1 : 0);
      doc.add(divVeteranField);

      Field divNativeField =
          new IntPoint("divNative", resume.isDivNative() ? 1 : 0);
      doc.add(divNativeField);

      Field divAsianField =
          new IntPoint("divAsian", resume.isDivAsian() ? 1 : 0);
      doc.add(divAsianField);

      Field needSponsorshipField =
          new IntPoint("needSponsorship", resume.isNeedSponsorship() ? 1 : 0);
      doc.add(needSponsorshipField);

      if (!StringUtils.isEmpty(resume.getCompanyCurrent())) {
        Field ccField =
            new StringField("cc", resume.getCompanyCurrent(), Field.Store.NO);
        doc.add(ccField);
      }

      if (!StringUtils.isEmpty(resume.getCompanyIdCurrent())) {
        Field cicField = new StringField("cic", resume.getCompanyIdCurrent(),
                                         Field.Store.NO);
        doc.add(cicField);
      }

      for (String companyPast : resume.getCompaniesPast()) {
        Field cpField = new StringField("cp", companyPast, Field.Store.NO);
        doc.add(cpField);
      }

      for (String companyIdPast : resume.getCompanyIdsPast()) {
        Field cipField = new StringField("cip", companyIdPast, Field.Store.NO);
        doc.add(cipField);
      }

      for (String industry : resume.getIndustries()) {
        Field industryField =
            new StringField("industry", industry, Field.Store.NO);
        doc.add(industryField);
      }

      if (!StringUtils.isEmpty(resume.getLocContinent())) {
        Field continentField = new StringField(
            "continent", resume.getLocContinent(), Field.Store.NO);
        doc.add(continentField);
      }

      if (!StringUtils.isEmpty(resume.getLocNation())) {
        Field nationField =
            new StringField("nation", resume.getLocNation(), Field.Store.NO);
        doc.add(nationField);
      }

      if (!StringUtils.isEmpty(resume.getLocState())) {
        Field stateField =
            new StringField("state", resume.getLocState(), Field.Store.NO);
        doc.add(stateField);
      }

      if (!StringUtils.isEmpty(resume.getLocCity())) {
        Field cityField =
            new StringField("city", resume.getLocCity(), Field.Store.NO);
        doc.add(cityField);
      }

      Field distanceField =
          new LatLonPoint("distance", resume.getLocLat(), resume.getLocLon());
      doc.add(distanceField);

      if (!StringUtils.isEmpty(resume.getLocRaw())) {
        Field locField =
            new TextField("loc", resume.getLocRaw(), Field.Store.NO);
        doc.add(locField);
      }

      if (!StringUtils.isEmpty(resume.getCompoundInfo())) {
        Field compoundField =
            new TextField("compound", resume.getCompoundInfo(), Field.Store.NO);
        doc.add(compoundField);
      }

      count++;
      // uidEmbeddingMap.put(resume.getUid(), resume.getEmbedding());
      jedisUtils.set(resume.getUid(), resume.getEmbedding());
      // logger.info("set uid->vector to pika,uid:"+resume.getUid());
      try {
        writer.addDocument(doc);
        //                if (count % 200 == 0 || count == resumes.size()) {
        //                    logger.info("documents flushed to disk, count: " +
        //                    count); writer.flush();
        //                }
      } catch (IOException e) {
        logger.error("fail to add document: " + resume.toString(), e);
      }
    }
    logger.info("add finish,num:"+resumes.size());
    //  logger.info("total docs: " + uidEmbeddingMap.size());
  }

  public synchronized void mergeSegments() {
    try {
      logger.info("OK ,I am going to merge,maybe it is horribly costly depends on index scale,please wait...");
      writer.forceMerge(1);
      writer.commit();
      writer.close();
      logger.info("force merge index done!!!");

      // build ivfpq index for vectors
      IndexReader reader = DirectoryReader.open(
          FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));

      List<LeafReaderContext> list = reader.leaves();
      
      if (list.size() > 1) {
        logger.error("error:more the one segment");
        return;
      }
      LeafReaderContext lrc = list.get(0);
      LeafReader lr = lrc.reader();
      int maxDocId = lr.maxDoc();
      int numDocs=lr.numDocs();
      if(maxDocId!=numDocs){
        logger.warn("stop add to jna,invalid document num for segment,numDocs: "+numDocs+",maxDoc:"+maxDocId);
        return;
      }else{
        logger.info(maxDocId+" documents to do clib index");
      }
      jedisUtils.init();
      int docId = 0;
      maxDocId=maxDocId-docId;

      int batchSize = 10000;//test to be appropriate value
      int integralBatch = maxDocId / batchSize;
      int remainBatchSize = maxDocId % batchSize;
      
      for (int i = 0; i < integralBatch; i++) {
        addVectors(lr,batchSize,docId);
        docId+=batchSize;
      }
      if(remainBatchSize>0){
        addVectors(lr,remainBatchSize,docId);
        docId+=remainBatchSize;
      }
      reader.close();
      jedisUtils.close();
      int success=clib.FilterKnn_Save(pFlatFile, pIvfpqFile);
      if(success!=1){
        logger.error("jna :FilterKnn_Save call error,msg:"+clib.FilterKnn_GetErrorMsg() );
        
      }
      logger.info("vector index done!!! total " + docId +
                   " embeddings indexed!!!");
    } catch (IOException e) {
      logger.error("fail to force merge index", e);
    }
  }
  /**
   *
   * @param size
   * @param docId
   * @return false if error occur
   * @throws IOException
   */
  private void addVectors(LeafReader lr, int size, int docIdStart) throws IOException {
    long s=System.currentTimeMillis();
    
    Set<String> uidField = new HashSet<>();
    uidField.add("uid");
    List<String>uids=new ArrayList<>();
    List<Integer>docIds=new ArrayList<>();

    logger.info("reading lucene...");
    long t=System.currentTimeMillis();
    for (int j = 0; j < size; j++) {

      Document doc = lr.document(docIdStart, uidField);

      if (doc != null) {
        String uid = doc.get("uid");
        uids.add(uid);
        docIds.add(docIdStart);
      } else {
        logger.warn("document is null for docId : " + docIdStart);
        // return false;
      }
      docIdStart++;
    }
    logger.info("done!cost:"+(System.currentTimeMillis()-t));

    int existDocSize = docIds.size();
    long id[] = new long[existDocSize];
    for (int i = 0; i < existDocSize; i++) {
      id[i] = docIds.get(i);
    }
    float[] vectors = new float[existDocSize * embeddingDimension];
    logger.info("reading pika...");
     t=System.currentTimeMillis();
    int batchSize = 1000;
    int integralBatch = existDocSize / batchSize;
    int remainBatchSize = existDocSize % batchSize;
    List<float[]> list=new ArrayList<>();
    int from=0;
    //can not get pika multithread for the uid is in order
    for (int i = 0; i < integralBatch; i++) {
      list.addAll(jedisUtils.mget(uids.subList(from, from+batchSize)));
      from+=batchSize;
    }
    if(remainBatchSize>0){
      list.addAll(jedisUtils.mget(uids.subList(from, from+remainBatchSize)));
      from+=remainBatchSize;
    }
   
    logger.info("done!cost:"+(System.currentTimeMillis()-t));
    for (int i = 0; i < list.size(); i++) {
      float[] v = list.get(i);

      if (v != null) {
        // float[] vector = uidEmbeddingMap.get(uid);
        for (int j = 0; j < embeddingDimension; j++) {
          vectors[i * embeddingDimension + j] = v[j];
        }

      } else {
        logger.warn("no vector found for uid: " + uids.get(i)+"，the corresponding docid:"+docIds.get(i)+
        "，and the corresponding vector will be filled with 0");
        // return false;
      }
    }
    logger.info("add vector...");
     t=System.currentTimeMillis();
    int success=clib.FilterKnn_AddVectors(vectors, id, size);
    if(success!=1){
      logger.warn("jna:FilterKnn_AddVectors call error,msg: "+clib.FilterKnn_GetErrorMsg() );
    }
    logger.info("done!cost:"+(System.currentTimeMillis()-t));
    logger.info("add vector,size:"+size+",end docid:"+docIdStart+",cost:"+(System.currentTimeMillis()-s));
  }
  public int commitAndCheckIndexSize(){
    try{
        writer.commit();
        IndexReader indexReader=DirectoryReader.open(FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
        int maxDoc=indexReader.maxDoc();
        int numDocs=indexReader.numDocs();
        if(maxDoc!=numDocs){
          logger.warn("invalid document num for index,numDocs: "+numDocs+",maxDoc:"+maxDoc);
        }
        return numDocs;
    }catch(Exception e){
        e.printStackTrace();
        return -1;
    }
   
}
  /**
   * close indexwriter anyway to make release writer lock
   */
  public static void shutDown(){
    try{
      writer.close();
    }catch(IOException e){
      e.printStackTrace();
    }
  
  }
}
