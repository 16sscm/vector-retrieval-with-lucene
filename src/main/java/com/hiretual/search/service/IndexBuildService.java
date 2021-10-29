package com.hiretual.search.service;

import com.hiretual.search.model.Resume;
import com.hiretual.search.utils.GlobalPropertyUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Service
public class IndexBuildService {

    private static final Logger logger = LoggerFactory.getLogger(IndexBuildService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");
    private static final String MAX_MEMORY = GlobalPropertyUtils.get("max.memory");

    private static Analyzer analyzer = new StandardAnalyzer();
    private static IndexWriter writer;
    private static int maxMemory;
    private static FieldType textFieldType = new FieldType(TextField.TYPE_NOT_STORED);

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
            writer = new IndexWriter(dir, iwc);
        } catch (IOException e) {
            logger.warn("fail to initialize index writer");
        }
    }

    public boolean addDocument(List<Resume> resumes) {

        //TODO: to be implemented
        return true;
    }
}
