package com.hiretual.search.service;

import com.hiretual.search.filterindex.*;
import com.hiretual.search.model.Resume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
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

@Service
public class IndexBuildService {

  private static final Logger logger =
      LoggerFactory.getLogger(IndexBuildService.class);

  private static final String USER_HOME = System.getProperty("user.home");
  private static final String INDEX_FOLDER =
      GlobalPropertyUtils.get("index.folder");
  private static final String MAX_MEMORY =
      GlobalPropertyUtils.get("max.memory");

  private static Analyzer analyzer = new StandardAnalyzer();
  private static IndexReader indexReader;
  private static IndexSearcher indexSearcher;
  private static IndexWriter writer;
  private static int maxMemory;
  private static FieldType textFieldType =
      new FieldType(TextField.TYPE_NOT_STORED);

  static String c_index_dir =
      USER_HOME + GlobalPropertyUtils.get("c_index_dir");
  static int dim = Integer.parseInt(GlobalPropertyUtils.get("vector_dim"));
  static String pFlatFile = c_index_dir + "/pFlatFile";
  static String pIvfpqFile = c_index_dir + "/ivfpq.bin";
  private static CLib clib = CLib.INSTANCE;
  static {
    textFieldType.setStoreTermVectors(true);
    textFieldType.setStoreTermVectorPositions(true);
    textFieldType.setStoreTermVectorOffsets(true);
    textFieldType.setStoreTermVectorPayloads(false);
    File file = new File(c_index_dir);
    if (!file.exists()) { //如果文件夹不存在
      file.mkdir();       //创建文件夹
    }
    try {
      maxMemory = Integer.parseInt(MAX_MEMORY);
    } catch (NumberFormatException e) {
      logger.warn("fail to initialize max memory for index writer");
    }
    try {
      logger.info(USER_HOME);
      Directory dir = FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER));
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      iwc.setRAMBufferSizeMB(maxMemory);
      indexReader = DirectoryReader.open(
          FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
      indexSearcher = new IndexSearcher(indexReader);
      writer = new IndexWriter(dir, iwc);
    } catch (IOException e) {
      logger.warn("fail to initialize index writer");
    }
    // load knn model(trained) and flat map if exist to make it  searchable,
    // otherwise you should  add the vector first,and remember to save
    clib.FilterKnn_InitLibrary(128, 10000, 64, 8, pFlatFile, pIvfpqFile);
  }
  /**
   * manually control
   */
  public void close() {

    try {
      //flush should be invoked by single thread to avoid that merge process miss some segment
      writer.flush();
      // It seems necessary to merge the segments,it is horribly costly,
      writer.forceMerge(1);
      //commit to make it searchable
      writer.commit();
      //close or open resources and release the write lock
      writer.close();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  public boolean addDocument(List<Resume> resumes) {

    // TODO: to be implemented
    return true;
  }
  /**
   * read ids from lucene by docid,get the corresponding vector from outside
   * add and save to both ann and bruteforce index
   */
  public void ivfpqFlatIndex() {

    int totalDocNum = indexReader.maxDoc();
    long ids[] = new long[totalDocNum];
    float[] vectors = new float[totalDocNum * dim];
    try {
      for (int i = 0; i < totalDocNum; i++) {
        Document document;

        document = indexSearcher.doc(i);

        String id = document.get("id");
        float[] vector = Utils.getRawMap().get(id);
        ids[i] = i;
        for (int j = 0; j < dim; j++) {
          vectors[i * dim + j] = vector[j];
        }
      }
      clib.FilterKnn_AddVectors(vectors, ids, totalDocNum);
      clib.FilterKnn_Save(pFlatFile, pIvfpqFile);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
