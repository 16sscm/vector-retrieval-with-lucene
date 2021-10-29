package com.hiretual.search.service;

import com.hiretual.search.utils.GlobalPropertyUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.IndexSearcher;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;


@Service
public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger(SearchService.class);

    private static final String USER_HOME = System.getProperty("user.home");
    private static final String INDEX_FOLDER = GlobalPropertyUtils.get("index.folder");

    private static final Analyzer analyzer = new StandardAnalyzer();
    private static IndexSearcher indexSearcher;

    static {
        try {
            logger.info(USER_HOME);
            indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(USER_HOME + INDEX_FOLDER))));
        } catch (IOException e) {
            logger.warn("fail to initialize index searcher");
        }
    }

}
