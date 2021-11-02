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
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;

@Service
public class IndexBuildService {

    private static final Logger logger = LoggerFactory.getLogger(IndexBuildService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    private static final String MAX_MEMORY = GlobalPropertyUtils.get("max.memory");
    private static final String EMBEDDING_DIMENSION = GlobalPropertyUtils.get("embedding.dimension");

    private static Analyzer analyzer = new StandardAnalyzer();
    private static IndexWriter writer;
    private static int maxMemory;
    private static Map<String, float[]> uidEmbeddingMap = new HashMap<>();
    public static int embeddingDimension;
    static String c_index_dir =
            USER_HOME + GlobalPropertyUtils.get("c_index_dir");
    static String pFlatFile = c_index_dir + "/pFlatFile";
    static String pIvfpqFile = c_index_dir + "/pIvfpqFile";
    private static CLib clib = CLib.INSTANCE;

    static {
        File file = new File(c_index_dir);
        if (!file.exists()) { //如果文件夹不存在
            file.mkdir();       //创建文件夹
        }
        try {
            maxMemory = Integer.parseInt(MAX_MEMORY);
            embeddingDimension = Integer.parseInt(EMBEDDING_DIMENSION);
        } catch(NumberFormatException e) {
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
        clib.FilterKnn_InitLibrary(128, 10000, 64, 8, pFlatFile, pIvfpqFile);
    }

    public void addDocument(List<Resume> resumes) {
        int count = 0;
        for (Resume resume : resumes) {
            Document doc = new Document();

            Field uidField = new StringField("uid", resume.getUid(), Field.Store.YES);
            doc.add(uidField);

            Field hotField4Sort = new NumericDocValuesField("score", resume.isHasPersonalEmail() ? resume.getAvailability() + 1000 : resume.getAvailability());
            doc.add(hotField4Sort);

            if (!StringUtils.isEmpty(resume.getDegree())) {
                Field degreeField = new StringField("degree", resume.getDegree(), Field.Store.NO);
                doc.add(degreeField);
            }

            if (!StringUtils.isEmpty(resume.getYoe())) {
                Field yoeField = new StringField("yoe", resume.getYoe(), Field.Store.NO);
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

            Field divWomanField = new IntPoint("divWoman", resume.isDivWoman() ? 1 : 0);
            doc.add(divWomanField);

            Field divBlackField = new IntPoint("divBlack", resume.isDivBlack() ? 1 : 0);
            doc.add(divBlackField);

            Field divHispanicField = new IntPoint("divHispanic", resume.isDivHispanic() ? 1 : 0);
            doc.add(divHispanicField);

            Field divVeteranField = new IntPoint("divVeteran", resume.isDivVeteran() ? 1 : 0);
            doc.add(divVeteranField);

            Field divNativeField = new IntPoint("divNative", resume.isDivNative() ? 1 : 0);
            doc.add(divNativeField);

            Field divAsianField = new IntPoint("divAsian", resume.isDivAsian() ? 1 : 0);
            doc.add(divAsianField);

            Field needSponsorshipField = new IntPoint("needSponsorship", resume.isNeedSponsorship() ? 1 : 0);
            doc.add(needSponsorshipField);

            if (!StringUtils.isEmpty(resume.getCompanyCurrent())) {
                Field ccField = new StringField("cc", resume.getCompanyCurrent(), Field.Store.NO);
                doc.add(ccField);
            }

            if (!StringUtils.isEmpty(resume.getCompanyIdCurrent())) {
                Field cicField = new StringField("cic", resume.getCompanyIdCurrent(), Field.Store.NO);
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
                Field industryField = new StringField("industry", industry, Field.Store.NO);
                doc.add(industryField);
            }

            if (!StringUtils.isEmpty(resume.getLocContinent())) {
                Field continentField = new StringField("continent", resume.getLocContinent(), Field.Store.NO);
                doc.add(continentField);
            }

            if (!StringUtils.isEmpty(resume.getLocNation())) {
                Field nationField = new StringField("nation", resume.getLocNation(), Field.Store.NO);
                doc.add(nationField);
            }

            if (!StringUtils.isEmpty(resume.getLocState())) {
                Field stateField = new StringField("state", resume.getLocState(), Field.Store.NO);
                doc.add(stateField);
            }

            if (!StringUtils.isEmpty(resume.getLocCity())) {
                Field cityField = new StringField("city", resume.getLocCity(), Field.Store.NO);
                doc.add(cityField);
            }

            Field distanceField = new LatLonPoint("distance", resume.getLocLat(), resume.getLocLon());
            doc.add(distanceField);

            if (!StringUtils.isEmpty(resume.getLocRaw())) {
                Field locField = new TextField("loc", resume.getLocRaw(), Field.Store.NO);
                doc.add(locField);
            }

            if (!StringUtils.isEmpty(resume.getCompoundInfo())) {
                Field compoundField = new TextField("compound", resume.getCompoundInfo(), Field.Store.NO);
                doc.add(compoundField);
            }

            count++;
            uidEmbeddingMap.put(resume.getUid(), resume.getEmbedding());
            try {
                writer.addDocument(doc);
//                if (count % 200 == 0 || count == resumes.size()) {
//                    logger.info("documents flushed to disk, count: " + count);
//                    writer.flush();
//                }
            } catch(IOException e) {
                logger.error("fail to add document: " + resume.toString(), e);
            }
        }
        logger.info("total docs: " + uidEmbeddingMap.size());
    }

    public synchronized void mergeSegments(){
        try {
            writer.forceMerge(1);
            writer.commit();
            writer.close();
            logger.error("force merge index done!!!");

            // build ivfpq index for vectors
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER)));
            Set<String> uidField = new HashSet<>();
            uidField.add("uid");
            List<LeafReaderContext> list = reader.leaves();
            int count = 0;
            for (LeafReaderContext lrc : list) {
                LeafReader lr = lrc.reader();
                int maxDocId = lr.maxDoc();
                for (int i = 0; i < maxDocId; i++) {
                    Document doc = lr.document(i, uidField);
                    if (doc != null) {
                        String uid = doc.get("uid");
                        if (uidEmbeddingMap.containsKey(uid)) {
                            logger.info("docID:" + i + "|uid:" + uid + "|vector:" + Arrays.toString(uidEmbeddingMap.get(uid)));
                            count++;
                        } else {
                            logger.warn("no vector found for uid: " + uid);
                        }
                    }
                }
            }
            reader.close();
            logger.error("vector index done!!! total " +  count + " embeddings indexed!!!");
        } catch(IOException e) {
            logger.error("fail to force merge index", e);
        }
    }
}
