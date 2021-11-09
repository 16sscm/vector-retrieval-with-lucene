package com.hiretual.search.web;

import com.hiretual.search.model.Resume;
import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.service.SearchService;
import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.utils.RequestParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
    public void insertDocument(HttpServletRequest request) {
        JsonNode array = RequestParser.getPostParameter(request);
        List<Resume> list = new ArrayList<>();
        for (JsonNode jn : array) {
            Resume resume = new Resume(jn);
            list.add(resume);
        }
        indexBuildService.addDocument(list);
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
    public String search(HttpServletRequest request, HttpServletResponse response) {
        //TODO: to be implemented
        return null;
    }
}
