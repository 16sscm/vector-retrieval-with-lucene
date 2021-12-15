package com.hiretual.search.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.filterindex.*;
import com.hiretual.search.model.DistributeInfo;
import com.hiretual.search.model.FilterResume;
import com.hiretual.search.model.Resume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import com.hiretual.search.utils.RawDataReader;
import com.hiretual.search.utils.RequestParser;
import com.hiretual.search.utils.RocksDBClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class IndexBuildService {

	private static final Logger logger = LoggerFactory.getLogger(IndexBuildService.class);

	private static final int threadPoolSize = 8; //TODO: put this into config file
	private static final int threadNumPerJson = 16; //TODO: put this into config file

	private static final String USER_HOME = System.getProperty("user.home");
	private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
	private static final String RAW_JSON = GlobalPropertyUtils.get("raw_json");
	private static final String EMBEDDING_JSON = GlobalPropertyUtils.get("embedding_json");

	private static final String MAX_MEMORY = GlobalPropertyUtils.get("max.memory");
	private static final String EMBEDDING_DIMENSION = GlobalPropertyUtils.get("embedding.dimension");
	private static String NUM_IVF_CLUSTER = GlobalPropertyUtils.get("num_ivf_cluster");

	private static Analyzer analyzer = new StandardAnalyzer();
	private static IndexWriter writer;
	private static int maxMemory;
	// private static Map<String, float[]> uidEmbeddingMap = new HashMap<>();
	public static int embeddingDimension;
	static String c_index_dir =
			USER_HOME + GlobalPropertyUtils.get("c_index_dir");


	static String pIvfpqFile = c_index_dir + GlobalPropertyUtils.get("ivfpq_file");
	public static int numIvfCluster;
	private static CLib clib = CLib.INSTANCE;

	@Autowired
	private RocksDBClient rocksDBClient;
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
			Directory dir = FSDirectory.open(Paths.get(INDEX_FOLDER));
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(maxMemory);
			writer = new IndexWriter(dir, iwc);
		} catch (IOException e) {
			logger.warn("fail to initialize index writer");
		}
		// load knn model(trained) and flat map if exist to make it  searchable,
		// otherwise you should  add the vector first,and remember to save
		int suc=clib.FilterKnn_InitLibrary(embeddingDimension, numIvfCluster, 2, 256,  pIvfpqFile);
		if(suc==0){
			logger.error("fail to initialize clib,msg:"+clib.FilterKnn_GetErrorMsg());
		}
	}

	private class AddDocThread extends Thread {
		private String id;
		private List<JsonNode> docs;
		private List<JsonNode> embeddings;

		public AddDocThread(String tid) {
			id = tid;
			docs = new ArrayList<>();
			embeddings = new ArrayList<>();
		}

		public AddDocThread(String tid, List<JsonNode> docList, List<JsonNode> embeddingList) {
			id = tid;
			docs = docList;
			embeddings = embeddingList;
		}

		public void addDoc(JsonNode d, JsonNode e) {
			docs.add(d);
			embeddings.add(e);
		}

		@Override
		public void run() {
			long t1 = System.currentTimeMillis();
			Map<String, float[]> embeddingMap = new HashMap<>();
			List<Document> docList = new ArrayList<>();
			for (int i = 0; i < docs.size(); i++){
				JsonNode doc = docs.get(i);
				Resume resume = new Resume(doc);

				JsonNode embeddingNode = embeddings.get(i);
				float[] embedding = new float[embeddingDimension];
				Iterator<JsonNode> arrayIterator = embeddingNode.iterator();
				int k = 0;
				while(arrayIterator.hasNext() && k < embeddingDimension) {
					embedding[k] = (float) arrayIterator.next().asDouble();
					k++;
				}
				embeddingMap.put(resume.getUid(), embedding);
				docList.add(convert2Document(resume));
			}
			long t2 = System.currentTimeMillis();
			long t3 = t2;
			long t4 = t2;
			if (docList.size() > 0 && docList.size() == embeddingMap.size()) {
				try{
					rocksDBClient.batchSet(embeddingMap);
				}catch(Exception e){
					logger.error("fail to batch set embedding|" + id, e);
				}
				t3 = System.currentTimeMillis();
				// logger.info("set uid->vector to pika,uid:"+resume.getUid());
				try {
					writer.addDocuments(docList);
				} catch (IOException e) {
					logger.error("fail to batch add documents|" + id, e);
				}
				t4 = System.currentTimeMillis();
				embeddingMap.clear();
				docList.clear();
			}
//			logger.info("thread " + id + " done! time cost: " + (t4 - t3) + "|" + (t3 - t2) + "|" + (t2 - t1));
		}

		private Document convert2Document(Resume resume){

			Document doc = new Document();

			addStringIntoDoc(doc, "uid", resume.getUid(), Field.Store.YES);

			addStringIntoDoc(doc, "yoe", resume.getYoe(), Field.Store.NO);
			addStringIntoDoc(doc, "seniority", resume.getSeniority(), Field.Store.NO);
			addIntPointIntoDoc(doc, "mcc", resume.getMonthsCurrentCompany());
			addIntPointIntoDoc(doc, "mcr", resume.getMonthsCurrentRole());

			addIntPointIntoDoc(doc, "divWoman", resume.isDivWoman() ? 1 : 0);
			addIntPointIntoDoc(doc, "divBlack", resume.isDivBlack() ? 1 : 0);
			addIntPointIntoDoc(doc, "divHispanic", resume.isDivHispanic() ? 1 : 0);
			addIntPointIntoDoc(doc, "divVeteran", resume.isDivVeteran() ? 1 : 0);
			addIntPointIntoDoc(doc, "divNative", resume.isDivNative() ? 1 : 0);
			addIntPointIntoDoc(doc, "divAsian", resume.isDivAsian() ? 1 : 0);

			addIntPointIntoDoc(doc, "hasPersonalEmail", resume.isHasPersonalEmail() ? 1 : 0);
			addIntPointIntoDoc(doc, "hasContact", resume.isHasPersonalEmail() ? 1 : 0);
			addIntPointIntoDoc(doc, "needSponsorship", resume.isNeedSponsorship() ? 1 : 0);

			if (resume.getItRankLevel() >= 0) {
				addIntPointIntoDoc(doc, "itRankLevel", resume.getItRankLevel());
			}
			if (resume.getEduGradYear() >= 0) {
				addIntPointIntoDoc(doc, "eduGradYear", resume.getEduGradYear());
			}
			addSetConcatStringIntoDoc(doc, "eduDegree", resume.getEduDegrees(), Field.Store.NO);
			addSetStringIntoDoc(doc, "eduLevel", resume.getEduLevels(), Field.Store.NO);
			addSetStringIntoDoc(doc, "eduBALK", resume.getEduBusinessAdmLevels(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "eduBAL", resume.getEduBusinessAdmLevels(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "eduMajor", resume.getEduMajors(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "eduSN", resume.getEduSchoolNames(), Field.Store.NO);
			addSetStringIntoDoc(doc, "eduSI", resume.getEduSchoolIds(), Field.Store.NO);

			addSetConcatStringIntoDoc(doc, "language", resume.getLanguages(), Field.Store.NO);

			addTextIntoDoc(doc, "cc", resume.getCompanyCurrent(), Field.Store.NO);
			addStringIntoDoc(doc, "cic", resume.getCompanyIdCurrent(), Field.Store.NO);
			addSetStringIntoDoc(doc, "csc", resume.getCompanySizeCurrent(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "cp", resume.getCompaniesPast(), Field.Store.NO);
			addSetStringIntoDoc(doc, "cip", resume.getCompanyIdsPast(), Field.Store.NO);
			addSetStringIntoDoc(doc, "industry", resume.getIndustries(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "tc", resume.getTitlesCurrent(), Field.Store.NO);
			addSetStringIntoDoc(doc, "ntcK", resume.getNormedTitlesCurrent(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "ntc", resume.getNormedTitlesCurrent(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "tp", resume.getTitlesPast(), Field.Store.NO); // not used for now
			addSetConcatStringIntoDoc(doc, "ntp", resume.getNormedTitlesPast(), Field.Store.NO); // not used for now
			addSetStringIntoDoc(doc, "nsK", resume.getNormedSkills(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "ns", resume.getNormedSkills(), Field.Store.NO);
			addSetConcatStringIntoDoc(doc, "rs", resume.getReviewedSkills(), Field.Store.NO);

			addTextIntoDoc(doc, "loc", resume.getLocRaw(), Field.Store.NO);
			addStringIntoDoc(doc, "locFMT", resume.getLocFmt(), Field.Store.NO);
			addStringIntoDoc(doc, "locType", resume.getLocType(), Field.Store.NO);
			addStringIntoDoc(doc, "continent", resume.getLocContinent(), Field.Store.NO);
			addStringIntoDoc(doc, "country", resume.getLocNation(), Field.Store.NO);
			addStringIntoDoc(doc, "state", resume.getLocState(), Field.Store.NO);
			addStringIntoDoc(doc, "city", resume.getLocCity(), Field.Store.NO); // not used for now
			Field distanceField = new LatLonPoint("distance", resume.getLocLat(), resume.getLocLon());
			doc.add(distanceField);

			addTextIntoDoc(doc, "ts", resume.getTitleSkill(), Field.Store.NO); // not used for now
			addTextIntoDoc(doc, "compound", resume.getCompoundInfo(), Field.Store.NO);

			return doc;
		}

		private void addSetConcatStringIntoDoc(Document doc, String fieldName, Set<String> values, Field.Store isStored) {
			if (values == null || values.size() == 0) {
				return;
			}
			String str = String.join(" , ", values);
			if (!StringUtils.isEmpty(str)) {
				Field field = new TextField(fieldName, str, isStored);
				doc.add(field);
			}
		}

		private void addSetStringIntoDoc(Document doc, String fieldName, Set<String> values, Field.Store isStored) {
			if (values == null || values.size() == 0) {
				return;
			}
			for (String value : values) {
				if (!StringUtils.isEmpty(value)) {
					Field field = new StringField(fieldName, value.toLowerCase(), isStored);
					doc.add(field);
				}
			}
		}

		private void addStringIntoDoc(Document doc, String fieldName, String value, Field.Store isStored) {
			if (StringUtils.isEmpty(value)) {
				return;
			}
			Field field = new StringField(fieldName, value.toLowerCase(), isStored);
			doc.add(field);
		}

		private void addSetTextIntoDoc(Document doc, String fieldName, Set<String> values, Field.Store isStored) {
			if (values == null || values.size() == 0) {
				return;
			}
			for (String value : values) {
				if (!StringUtils.isEmpty(value)) {
					Field field = new TextField(fieldName, value, isStored);
					doc.add(field);
				}
			}
		}

		private void addTextIntoDoc(Document doc, String fieldName, String value, Field.Store isStored) {
			if (StringUtils.isEmpty(value)) {
				return;
			}
			Field field = new TextField(fieldName, value, isStored);
			doc.add(field);
		}

		private void addNumericIntoDoc(Document doc, String fieldName, int value) {
			Field field = new NumericDocValuesField(fieldName, value);
			doc.add(field);
		}

		private void addIntPointIntoDoc(Document doc, String fieldName, int value) {
			Field field = new IntPoint(fieldName, value);
			doc.add(field);
		}
	}

	private class SingleJsonFileTask implements Runnable {
		private String jsonFilePath;
		private String embeddingFilePath;

		public SingleJsonFileTask(String jfp, String efp) {
			jsonFilePath = jfp;
			embeddingFilePath = efp;
		}

		@Override
		public void run() {
			long t1 = System.currentTimeMillis();
			JsonNode docs=RequestParser.getPostParameter(RawDataReader.readJsonFile(jsonFilePath));
			long t2 = System.currentTimeMillis();
			JsonNode embeddings=RequestParser.getPostParameter(RawDataReader.readJsonFile(embeddingFilePath)).get("embedding");
			long t3 = System.currentTimeMillis();
			if(docs.size()!=embeddings.size()){
				logger.warn("invalid raw data,raw data json does not match embedding json,json file:"+jsonFilePath);
			}
			List<AddDocThread> threadList = new ArrayList<>();
			for (int j = 0; j < threadNumPerJson; j++) {
				threadList.add(new AddDocThread(jsonFilePath + "___" + j));
			}
			for(int j = 0; j < docs.size(); j++){
				threadList.get(j % threadNumPerJson).addDoc(docs.get(j), embeddings.get(j));
			}
			for (int j = 0; j < threadNumPerJson; j++) {
				threadList.get(j).start();
			}
			for (int j = 0; j < threadNumPerJson; j++) {
				try {
					threadList.get(j).join();
				} catch (InterruptedException e) {
					logger.error("thread interrupted", e);
				}
			}
			threadList.clear();
			long t4 = System.currentTimeMillis();
			logger.info("add "+embeddings.size()+" to lucene and rocksDB,json file: "+jsonFilePath + "|" + (t4 - t3) + "|" + (t3 - t2) + "|" + (t2 - t1));
		}
	}

	public void process(DistributeInfo distributeInfo){
		IndexBuilderHelper helper=new IndexBuilderHelper(distributeInfo);
		List<String>rawJsonList=helper.getRawJsonList();
		List<String>embeddingList=helper.getEmbeddingJsonList();

		ExecutorService es = Executors.newFixedThreadPool(threadPoolSize);
		for(int i=0;i<rawJsonList.size();i++){
			String rawJsonFilename=RAW_JSON+rawJsonList.get(i);
			String embeddingFilename=EMBEDDING_JSON+embeddingList.get(i);


			es.submit(new SingleJsonFileTask(rawJsonFilename, embeddingFilename));
		}
		es.shutdown();
		try{
			writer.commit();

		}catch(IOException e){
			logger.error("fail to commit indexwriter", e);
		}
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
					FSDirectory.open(Paths.get(INDEX_FOLDER)));

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


			int docId = 0;
			maxDocId=maxDocId-docId;

			int batchSize = 5000;//test to be appropriate value,to large will make pika error
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

			int success=clib.FilterKnn_Save(pIvfpqFile);
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
		FilterResume filterResumes[]=new FilterResume[existDocSize];
		for (int i = 0; i < existDocSize; i++) {
			filterResumes[i]=new FilterResume(uids.get(i));
			id[i] = docIds.get(i);
		}
		float[] vectors = new float[existDocSize * embeddingDimension];
		logger.info("reading rocksDB...");
		t=System.currentTimeMillis();
		int batchSize = 1000;
		int integralBatch = existDocSize / batchSize;
		int remainBatchSize = existDocSize % batchSize;
		List<float[]> list=new ArrayList<>();
		int from=0;
		//can not get rocksDB multithread for the uid is in order
		for (int i = 0; i < integralBatch; i++) {
			list.addAll(rocksDBClient.multiGetAsList(uids.subList(from, from+batchSize)));
			from+=batchSize;
		}
		if(remainBatchSize>0){
			list.addAll(rocksDBClient.multiGetAsList(uids.subList(from, from+remainBatchSize)));
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
		String filterResumeJson=RequestParser.getJsonString(filterResumes);
		int success=clib.FilterKnn_AddVectors(vectors, id, size,filterResumeJson);
		if(success!=1){
			logger.warn("jna:FilterKnn_AddVectors call error,msg: "+clib.FilterKnn_GetErrorMsg() );
		}
		logger.info("done!cost:"+(System.currentTimeMillis()-t));
		logger.info("add vector,size:"+size+",end docid:"+docIdStart+",cost:"+(System.currentTimeMillis()-s));
	}

	/**
	 * commit directory to searcher and return the index size
	 * @return
	 */
	public int commitAndCheckIndexSize(){
		    SearchService.lazyInit(writer);
			IndexReader indexReader=SearchService.indexReader;
			int maxDoc=indexReader.maxDoc();
			int numDocs=indexReader.numDocs();
			if(maxDoc!=numDocs){
				logger.warn("invalid document num for index,numDocs: "+numDocs+",maxDoc:"+maxDoc);
			}
			return numDocs;


	}
	public void deleteResume(Resume resume){
		String uid=resume.getUid();
		Query query=new TermQuery(new Term("uid",uid));
		TopDocs topDocs;
		try {
			IndexSearcher indexSearcher = SearchService.indexSearcher;
			topDocs = indexSearcher.search(query, 1);
			if(topDocs.totalHits.value!=1){
				logger.warn("resume do not exist on this index,uid:",uid);
			}else{
				int docId=topDocs.scoreDocs[0].doc;
				long []id=new long[]{docId};
				clib.FilterKnn_RemoveVectors(id, 1);
			}

		} catch (IOException e) {
			e.printStackTrace();

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
