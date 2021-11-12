package com.hiretual.search.web;

import com.hiretual.search.filterindex.KNNQuery;
import com.hiretual.search.filterindex.KNNResult;
import com.hiretual.search.model.Resume;
import com.hiretual.search.service.IndexBuildService;
import com.hiretual.search.service.SearchService;
import com.fasterxml.jackson.databind.JsonNode;
import com.hiretual.search.utils.QueryConvertor;
import com.hiretual.search.utils.RequestParser;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
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
        return "ok";
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
    public String search(@RequestBody JsonNode query) {
        long t1 = System.currentTimeMillis();
        JsonNode esQuery = QueryConvertor.postForEntityWithHeader(query.toString());
        // logger.info(esQuery.toString());
        long t2 = System.currentTimeMillis();
        List<Query> queries = QueryConvertor.convertESQuery(esQuery);
        long t3 = System.currentTimeMillis();
        if(queries==null){
            return null;
        }
       
       
        int k = 1000;
        double[] vec = new double[]{
            0.40318623185157776,
            -0.7447794675827026,
            0.6329336166381836,
            0.8054167628288269,
            0.0939868837594986,
            1.9087612628936768,
            2.640871286392212,
            1.7870702743530273,
            -0.8907918930053711,
            -1.5092071294784546,
            2.0455758571624756,
            0.5398554801940918,
            -1.029198408126831,
            1.5186896324157715,
            -0.4830380380153656,
            3.556234836578369,
            1.2950737476348877,
            -2.2208924293518066,
            -0.5998330116271973,
            2.316866159439087,
            -0.5960012674331665,
            -0.9348864555358887,
            -1.8130728006362915,
            1.560150146484375,
            1.7493535280227661,
            -0.7021516561508179,
            0.5321575403213501,
            2.1616597175598145,
            -1.1731846332550049,
            -3.461344003677368,
            -2.8547704219818115,
            -0.2305520623922348,
            0.05581925809383392,
            -1.8541232347488403,
            0.4135010838508606,
            0.1279725581407547,
            0.1908843219280243,
            -0.9398955702781677,
            0.7444708943367004,
            -0.7727982997894287,
            1.8512099981307983,
            2.5141243934631348,
            1.633852481842041,
            -0.46272793412208557,
            1.5893161296844482,
            -0.6432779431343079,
            -0.02601654641330242,
            -1.4595966339111328,
            -0.3423241078853607,
            -0.5671008825302124,
            -0.07969973981380463,
            -0.036766763776540756,
            -0.7818107604980469,
            -1.5081292390823364,
            -0.21392899751663208,
            2.8736133575439453,
            -0.29071423411369324,
            0.7332969307899475,
            1.0023890733718872,
            -1.0522949695587158,
            -0.48004382848739624,
            2.3800711631774902,
            -0.9411724805831909,
            -0.24571731686592102,
            0.5851773023605347,
            1.8978121280670166,
            1.0900071859359741,
            0.34325796365737915,
            -2.179887533187866,
            0.28172674775123596,
            -0.8611387610435486,
            2.100107192993164,
            -0.6596558094024658,
            -0.8368726968765259,
            0.09423479437828064,
            -1.4912360906600952,
            2.1840455532073975,
            -2.077221393585205,
            -0.49181440472602844,
            4.173897743225098,
            -0.43787965178489685,
            -1.8977078199386597,
            0.037848930805921555,
            -1.9503389596939087,
            1.291676640510559,
            -0.46867847442626953,
            -0.5676421523094177,
            -0.23822391033172607,
            -0.6214795112609863,
            2.349334955215454,
            1.8401308059692383,
            1.1847707033157349,
            2.6201791763305664,
            -1.1923213005065918,
            0.3581465482711792,
            -0.019589077681303024,
            -0.6174258589744568,
            -1.5003485679626465,
            -0.8658158183097839,
            0.7588832974433899,
            -2.964768171310425,
            1.533850908279419,
            -1.3400930166244507,
            -1.6363829374313354,
            -1.4998927116394043,
            -1.7574892044067383,
            -0.7283121943473816,
            -1.2132019996643066,
            0.4953349232673645,
            -1.0638331174850464,
            -2.511780261993408,
            -1.95734441280365,
            -0.7131657600402832,
            2.453568696975708,
            0.38353437185287476,
            3.7436506748199463,
            0.822658360004425,
            1.7933918237686157,
            -0.7265053391456604,
            0.7863985300064087,
            0.5780242085456848,
            -0.03355824947357178,
            0.09083503484725952,
            -0.9554247856140137,
            -1.6834763288497925,
            2.65315580368042,
            0.1403672844171524,
            0.17911523580551147};
        float[] v = new float[vec.length];
        for (int i = 0; i < vec.length; i++) {
            v[i] = (float)vec[i];
        }
        KNNQuery kq = new KNNQuery( v);
        //TODO: to be implemented
        try {
            KNNResult[] result = searchService.search(queries, kq, k);
            if(result==null){
                logger.info("0 results");
                return "";
            }
            logger.info(result.length + " results");
        } catch(Exception e) {
            logger.error("search error", e);
        }
        long end = System.currentTimeMillis();
        logger.info("total cost: " + (end - t3) + "|" + (t3-t2) +"|" +(t2-t1) + " ms");
        return "done";
    }
}
