package com.hiretual.search.web;

import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.service.SearchService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hiretual.search.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
@RestController
public class SearchController {
    private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

    @Autowired
    IndexBuildService indexBuildService;

    @Autowired
    SearchService searchService;

    @RequestMapping(value="/index", method=RequestMethod.GET)
    public String insertDocument(HttpServletRequest request, HttpServletResponse response) {
        // indexBuildService.addDocument(resumes)
        //TODO: to be implemented
        return null;
    }
    @RequestMapping(value="/finish", method=RequestMethod.GET)
    public String finishIndexing() {
        indexBuildService.close();
        //TODO: to be implemented
        return null;
    }
    @RequestMapping(value="/knn-index", method=RequestMethod.GET)
    public String ivfpqFlatIndex() {
        indexBuildService.ivfpqFlatIndex();
        //TODO: to be implemented
        return null;
    }

    @RequestMapping(value="/search", method=RequestMethod.POST)
    public String search(HttpServletRequest request, HttpServletResponse response) {
        //TODO: to be implemented
        return null;
    }
    @RequestMapping(value="/" ,method = RequestMethod.GET)
    public String hello(){
        return "hello world";
    }
}
