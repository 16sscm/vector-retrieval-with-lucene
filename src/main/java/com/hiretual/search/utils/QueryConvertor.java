package com.hiretual.search.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.*;

public class QueryConvertor {

    private static final Logger logger = LoggerFactory.getLogger(QueryConvertor.class);

    public static List<Query> convertESQuery(JsonNode esQuery) {
        List<Query> ret = new ArrayList<>();
        BooleanQuery.Builder excludebq = new BooleanQuery.Builder();
        MatchAllDocsQuery madq = new MatchAllDocsQuery();
        excludebq.add(madq, BooleanClause.Occur.SHOULD);
        int excludeCount = 0;
        Analyzer analyzer = new StandardAnalyzer();
        QueryBuilder queryBuilder = new QueryBuilder(analyzer);

        if (esQuery != null && !esQuery.isNull() && esQuery.has("bool")) {
            JsonNode boolNode = esQuery.get("bool");
            if (!boolNode.isNull() && boolNode.has("must")) {
                JsonNode mNode = boolNode.get("must");
                if (!mNode.isNull() && !mNode.isEmpty() && mNode.isArray()) {
                    for (JsonNode m : mNode) {
                        if (!m.findPath("user.healthcare.license_state").isMissingNode() ||
                            !m.findPath("user.healthcare.specialties").isMissingNode() ||
                            !m.findPath("user.healthcare.titles").isMissingNode()) {
                            return null;
                        } else if (!m.findPath("query_string").isMissingNode()) {
                            // search in multiple fields, including some healthcare fields
                            return null;
                        } else {
                            logger.warn("unknown field in must node: " + m.toString());
                        }
                    }
                } else {
                    logger.warn("unexpected must node: " + mNode.toString());
                }
            }

            if (!boolNode.isNull() && boolNode.has("filter")) {
                JsonNode filterNode = boolNode.get("filter");
                if (!filterNode.isNull() && !filterNode.isEmpty() && filterNode.isArray()) {
                    int eduGradYearFrom = -1;
                    int eduGradYearTo = -1;
                    for (JsonNode node : filterNode) {
                        if (!node.isNull() && node.has("term")) {
                            JsonNode termNode = node.get("term");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                logger.warn("empty term node");
                            } else if (termNode.has("user.tags.has_personal_email") && getFieldsCount(termNode) == 1) {
                                ret.add(IntPoint.newExactQuery("hasPersonalEmail", 1));
                            } else if (termNode.has("user.tags.race") && getFieldsCount(termNode) == 1) {
                                String race = termNode.get("user.tags.race").get("value").asText();
                                if (race.equals("african-american")) {
                                    ret.add(IntPoint.newExactQuery("divBlack", 1));
                                } else if (race.equals("hispanic")) {
                                    ret.add(IntPoint.newExactQuery("divHispanic", 1));
                                } else if (race.equals("native-american")) {
                                    ret.add(IntPoint.newExactQuery("divNative", 1));
                                } else if (race.equals("asian-american")) {
                                    ret.add(IntPoint.newExactQuery("divAsian", 1));
                                } else {
                                    logger.warn("sth strange!!!" + termNode.toString());
                                }
                            } else if (termNode.has("user.location.country.keyword") && getFieldsCount(termNode) == 1) {
                                ret.add(new TermQuery(new Term("country", termNode.get("user.location.country.keyword").get("value").asText().toLowerCase())));
                            } else {
                                logger.warn("unexpected term query: " + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("terms")) {
                            JsonNode termNode = node.get("terms");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                logger.warn("empty terms node");
                            } else if (termNode.has("user.tags.seniority") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termssbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode seniority : termNode.get("user.tags.seniority")) {
                                    termssbq.add(new TermQuery(new Term("seniority", seniority.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termssbq.build());
                                }
                            } else if (termNode.has("user.tags.security_clearance.keyword") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                return null;
                            } else if (termNode.has("user.current_experience.normed_titles") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termsntcbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode ntc : termNode.get("user.current_experience.normed_titles")) {
                                    termsntcbq.add(new TermQuery(new Term("ntcK", ntc.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termsntcbq.build());
                                }
                            } else if (termNode.has("user.current_experience.company_size") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termscscbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode csc : termNode.get("user.current_experience.company_size")) {
                                    termscscbq.add(new TermQuery(new Term("csc", csc.asText())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termscscbq.build());
                                }
                            } else if (termNode.has("user.info_tech.rank_level") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                Set<Integer> inRankLevels = new HashSet<>();
                                for (JsonNode it : termNode.get("user.info_tech.rank_level")) {
                                    inRankLevels.add(Integer.parseInt(it.asText()));
                                }
                                ret.add(IntPoint.newSetQuery("itRankLevel", inRankLevels));
                            } else if (termNode.has("user.current_experience.company_id") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termscicbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode cic : termNode.get("user.current_experience.company_id")) {
                                    termscicbq.add(new TermQuery(new Term("cic", cic.asText())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termscicbq.build());
                                }
                            } else if (termNode.has("user.past_experience.company_id") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termscipbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode cip : termNode.get("user.past_experience.company_id")) {
                                    termscipbq.add(new TermQuery(new Term("cip", cip.asText())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termscipbq.build());
                                }
                            } else if (termNode.has("user.normed_skills") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termsnsbq = new BooleanQuery.Builder();
                                int mcounter  = 0;
                                for (JsonNode ns : termNode.get("user.normed_skills")) {
                                    termsnsbq.add(new TermQuery(new Term("nsK", ns.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termsnsbq.build());
                                }
                            } else if (termNode.has("user.location.location_fmt.keyword") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                BooleanQuery.Builder termslocbq = new BooleanQuery.Builder();
                                int mcounter = 0;
                                for (JsonNode loc : termNode.get("user.location.location_fmt.keyword")) {
                                    termslocbq.add(new TermQuery(new Term("locFMT", loc.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                    mcounter++;
                                }
                                if (mcounter > 0) {
                                    ret.add(termslocbq.build());
                                }
                            } else {
                                logger.warn("unexpected term query: " + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("exists")) {
                            JsonNode termNode = node.get("exists");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                logger.warn("empty exists node");
                            } else if (termNode.has("field") && termNode.get("field").asText().equals("user.info_tech") && getFieldsCount(termNode) == 2) {
                                // check if itRankLevel exists
                                ret.add(IntPoint.newRangeQuery("itRankLevel", 0, 100));
                            } else if (termNode.has("field") && termNode.get("field").asText().equals("user.healthcare") && getFieldsCount(termNode) == 2) {
                                return null;
                            } else if (termNode.has("field") && termNode.get("field").asText().equals("user.publication.title") && getFieldsCount(termNode) == 2) {
                                return null;
                            } else {
                                logger.warn("unexpected terms query: " + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("range")) {
                            JsonNode rangeNode = node.get("range");
                            if (rangeNode.isNull() || rangeNode.isEmpty()) {
                                logger.warn("empty range node");
                            } else if (rangeNode.hasNonNull("user.education.grad_year") && getFieldsCount(rangeNode) == 1) {
                                if (rangeNode.get("user.education.grad_year").hasNonNull("from")) {
                                    eduGradYearFrom = rangeNode.get("user.education.grad_year").get("from").asInt();
                                }
                                if (rangeNode.get("user.education.grad_year").hasNonNull("to")) {
                                    eduGradYearTo = rangeNode.get("user.education.grad_year").get("to").asInt();
                                }
                            } else if (rangeNode.has("user.tags.compensation_base_avg") && getFieldsCount(rangeNode) == 1) {
                                return null;
                            } else {
                                logger.warn("unexpected range query: " + rangeNode.toString());
                            }
                        } else if (!node.isNull() && node.has("query_string")) {
                            JsonNode termNode = node.get("query_string");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                logger.warn("empty query_string node");
                            } else if (termNode.has("query") && termNode.get("default_field").asText().equals("user.global_search")) {
                                QueryParser qp = new QueryParser("compound", analyzer);
                                if (termNode.get("default_operator").asText().equals("and")) {
                                    qp.setDefaultOperator(QueryParser.AND_OPERATOR);
                                } else {
                                    qp.setDefaultOperator(QueryParser.OR_OPERATOR);
                                }
                                String qs = termNode.get("query").asText();
                                try {
                                    Query qsq = qp.parse(qs);
                                    ret.add(qsq);
                                } catch (ParseException e) {
                                    logger.error("can not parse query_string, maybe broken: " + qs, e);
                                    return null;
                                }
                            } else {
                                logger.warn("unexpected query_string query: " + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("bool")) {
                            JsonNode bNode = node.get("bool");
                            if (!bNode.isNull() && !bNode.isEmpty()) {
                                int count = 0;
                                if (bNode.hasNonNull("should")) {
                                    JsonNode shouldNode = bNode.get("should");
                                    int c = 0;
                                    if (!shouldNode.findPath("user.client_id").isMissingNode() ||
                                        !shouldNode.findPath("user.sourced_client_ids").isMissingNode() ||
                                        !shouldNode.findPath("user.sourcing_channels.keyword").isMissingNode()) {
                                        // skip this part
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.title_skill_search").isMissingNode()) {
                                        // skip this, titles and skills have been embedded
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.location.location_value").isMissingNode() ||
                                        !shouldNode.findPath("user.location.continent.keyword").isMissingNode() ||
                                        !shouldNode.findPath("user.location.country.keyword").isMissingNode() ||
                                        !shouldNode.findPath("geo_distance").isMissingNode()) {
                                        BooleanQuery.Builder lbq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        Set<String> locValueSet = new HashSet<>();
                                        Set<String> locObjectSet = new HashSet<>();
                                        for (JsonNode loc : shouldNode) {
                                            if (!loc.findPath("user.location.location_value").isMissingNode()) {
                                                String locValue = loc.get("match_phrase").get("user.location.location_value").get("query").asText();
                                                if (!locValueSet.contains(locValue.toLowerCase())) {
                                                    lbq.add(queryBuilder.createPhraseQuery("loc", locValue), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                    locValueSet.add(locValue.toLowerCase());
                                                }
                                            } else if (loc.hasNonNull("bool")) {
                                                JsonNode lboolNode = loc.get("bool");
                                                BooleanQuery.Builder lmbq = new BooleanQuery.Builder();
                                                StringBuilder lsb = new StringBuilder();
                                                if (lboolNode.has("must")) {
                                                    for (JsonNode must : lboolNode.get("must")) {
                                                        if (!must.findPath("user.location.continent.keyword").isMissingNode()) {
                                                            String conti = must.get("term").get("user.location.continent.keyword").get("value").asText();
                                                            lsb.append('+').append(conti);
                                                            lmbq.add(new TermQuery(new Term("continent", conti.toLowerCase())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.country.keyword").isMissingNode()) {
                                                            String country = must.get("term").get("user.location.country.keyword").get("value").asText();
                                                            lsb.append('+').append(country);
                                                            lmbq.add(new TermQuery(new Term("country", country.toLowerCase())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.state.keyword").isMissingNode()) {
                                                            String state = must.get("term").get("user.location.state.keyword").get("value").asText();
                                                            lsb.append('+').append(state);
                                                            lmbq.add(new TermQuery(new Term("state", state.toLowerCase())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.geo_coordinates").isMissingNode()) {
                                                            JsonNode latlon = must.get("geo_distance").get("user.location.geo_coordinates");
                                                            lsb.append('+').append('(').append(latlon.get(1).asDouble()).append(',').append(latlon.get(0).asDouble()).append(')').append('+').append(must.get("geo_distance").get("distance").asInt());
                                                            lmbq.add(LatLonPoint.newDistanceQuery("distance", latlon.get(1).asDouble(), latlon.get(0).asDouble(), must.get("geo_distance").get("distance").asInt()), BooleanClause.Occur.MUST);
                                                        } else {
                                                            logger.warn("unexpected geo field: " + must.toString());
                                                        }
                                                    }
                                                }
                                                if (lboolNode.has("must_not")) {
                                                    for (JsonNode mustnot : lboolNode.get("must_not")) {
                                                        if (!mustnot.findPath("user.location.type.keyword").isMissingNode()) {
                                                            String locType = mustnot.get("term").get("user.location.type.keyword").get("value").asText();
                                                            lsb.append("-type:").append(locType);
                                                            lmbq.add(new TermQuery(new Term("locType", locType)), BooleanClause.Occur.MUST_NOT);
                                                        } else {
                                                            logger.warn("unknown loaction must_not node: " + mustnot.toString());
                                                        }
                                                    }
                                                }
                                                if (!locObjectSet.contains(lsb.toString().toLowerCase())) {
                                                    lbq.add(lmbq.build(), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                    locObjectSet.add(lsb.toString().toLowerCase());
                                                }
                                            } else {
                                                logger.warn("unexpected loc filter: " + loc.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(lbq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.current_experience.titles").isMissingNode() ||
                                        !shouldNode.findPath("user.current_experience.normed_titles").isMissingNode() ||
                                        !shouldNode.findPath("user.past_experience.titles").isMissingNode() ||
                                        !shouldNode.findPath("user.past_experience.normed_titles").isMissingNode()) {
                                        // skip this, titles and skills have been embedded
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.languages").isMissingNode()) {
                                        BooleanQuery.Builder languagebq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode lanNode : shouldNode) {
                                            String lanTag = lanNode.findPath("user.languages").get("query").asText();
                                            languagebq.add(queryBuilder.createPhraseQuery("language", lanTag), BooleanClause.Occur.SHOULD);
                                            mcounter++;
                                        }
                                        if (mcounter > 0) {
                                            ret.add(languagebq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.current_experience.company_types").isMissingNode()) {
                                        return null;
                                    }
                                    if (!shouldNode.findPath("user.current_experience.funding_round").isMissingNode()) {
                                        return null;
                                    }
                                    if (!shouldNode.findPath("user.tags.experience_tag").isMissingNode()) {
                                        if (shouldNode.size() > 1) {
                                            logger.warn("multi field in should yoe json: " + shouldNode.toString());
                                        }
                                        BooleanQuery.Builder yoebq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode yoe : shouldNode.get(0).get("terms").get("user.tags.experience_tag")) {
                                            yoebq.add(new TermQuery(new Term("yoe", yoe.asText())), BooleanClause.Occur.SHOULD);
                                            mcounter++;
                                        }
                                        if (mcounter > 0) {
                                            ret.add(yoebq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.current_experience.industries.keyword").isMissingNode()) {
                                        BooleanQuery.Builder industrybq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode industry : shouldNode) {
                                            String industryTag = industry.get("term").get("user.current_experience.industries.keyword").get("value").asText();
                                            industrybq.add(new TermQuery(new Term("industry", industryTag.toLowerCase())), BooleanClause.Occur.SHOULD);
                                            mcounter++;
                                        }
                                        if (mcounter > 0) {
                                            ret.add(industrybq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.current_experience.company_id").isMissingNode() ||
                                        !shouldNode.findPath("user.past_experience.company_id").isMissingNode() ||
                                        !shouldNode.findPath("user.current_experience.companies").isMissingNode() ||
                                        !shouldNode.findPath("user.past_experience.companies").isMissingNode()) {
                                        BooleanQuery.Builder cbq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for (JsonNode company : shouldNode) {
                                            if (!company.findPath("user.current_experience.company_id").isMissingNode()) {
                                                for (JsonNode ci : company.findPath("user.current_experience.company_id")) {
                                                    cbq.add(new TermQuery(new Term("cic", ci.asText())), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                }
                                            } else if (!company.findPath("user.past_experience.company_id").isMissingNode()) {
                                                for (JsonNode ci : company.findPath("user.past_experience.company_id")) {
                                                    cbq.add(new TermQuery(new Term("cip", ci.asText())), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                }
                                            } else if (!company.findPath("user.current_experience.companies").isMissingNode()) {
                                                String companyName = company.findPath("user.current_experience.companies").get("query").asText();
                                                cbq.add(queryBuilder.createPhraseQuery("cc", companyName), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else if (!company.findPath("user.past_experience.companies").isMissingNode()) {
                                                String companyName = company.findPath("user.past_experience.companies").get("query").asText();
                                                cbq.add(queryBuilder.createPhraseQuery("cp", companyName), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else {
                                                logger.warn("unexpected field found in company node: " + company.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(cbq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.education.schools").isMissingNode() ||
                                        !shouldNode.findPath("user.education.education_id").isMissingNode()) {
                                        BooleanQuery.Builder ebq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for (JsonNode education : shouldNode) {
                                            if (!education.findPath("user.education.schools").isMissingNode()) {
                                                String schoolName = education.findPath("user.education.schools").get("query").asText();
                                                ebq.add(queryBuilder.createPhraseQuery("eduSN", schoolName), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else if (!education.findPath("user.education.education_id").isMissingNode()) {
                                                for (JsonNode eid : education.findPath("user.education.education_id")) {
                                                    ebq.add(new TermQuery(new Term("eduSI", eid.asText())), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                }
                                            } else {
                                                logger.warn("unexpected field found in education node: " + education.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(ebq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.education.majors").isMissingNode() ||
                                        !shouldNode.findPath("user.education.degrees").isMissingNode()) {
                                        BooleanQuery.Builder majorbq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode major : shouldNode) {
                                            JsonNode terms = major.findPath("should");
                                            if (terms.size() > 2) {
                                                logger.warn("too many fields in education major json: " + terms.toString());
                                            }
                                            for (JsonNode maj : terms) {
                                                if (!maj.findPath("user.education.majors").isMissingNode()) {
                                                    String majName = maj.findPath("user.education.majors").get("query").asText();
                                                    majorbq.add(queryBuilder.createPhraseQuery("eduMajor", majName), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else if (!maj.findPath("user.education.degrees").isMissingNode()) {
                                                    String degreeName = maj.findPath("user.education.degrees").get("query").asText();
                                                    majorbq.add(queryBuilder.createPhraseQuery("eduDegree", degreeName), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth unexpected!!! " + terms.toString());
                                                }
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(majorbq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.tags.gender").isMissingNode() ||
                                        !shouldNode.findPath("user.tags.race").isMissingNode() ||
                                        !shouldNode.findPath("user.tags.veteran").isMissingNode()) {
                                        BooleanQuery.Builder diversitybq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode diversity : shouldNode) {
                                            if (!diversity.findPath("user.tags.gender").isMissingNode()) {
                                                if (diversity.findPath("user.tags.gender").get("value").asText().equals("female")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divWoman", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.race").isMissingNode()) {
                                                String race = diversity.findPath("user.tags.race").get("value").asText();
                                                if (race.equals("african-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divBlack", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else if (race.equals("hispanic")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divHispanic", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else if (race.equals("native-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divNative", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else if (race.equals("asian-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divAsian", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.veteran").isMissingNode()) {
                                                if (diversity.findPath("user.tags.veteran").get("value").asBoolean()) {
                                                    diversitybq.add(IntPoint.newExactQuery("divVeteran", 1), BooleanClause.Occur.SHOULD);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else {
                                                logger.warn("unexpected diversity field!!!" + diversity.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(diversitybq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.tags.current_company_start_date").isMissingNode()) {
                                        BooleanQuery.Builder dateRangebq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode dateRange : shouldNode) {
                                            JsonNode dr = dateRange.get("range").get("user.tags.current_company_start_date");
                                            String fromDate = dr.get("from").isNull() ? "" : dr.get("from").asText();
                                            String toDate = dr.get("to").isNull() ? "" : dr.get("to").asText();
                                            int lowerLimit = getFirstNum(toDate);
                                            int upperLimit = getFirstNum(fromDate);
                                            if (lowerLimit >= 0 & upperLimit > lowerLimit) {
                                                dateRangebq.add(IntPoint.newRangeQuery("mcc", lowerLimit, upperLimit), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else if (lowerLimit > 0 & upperLimit == 0) {
                                                dateRangebq.add(IntPoint.newRangeQuery("mcc", lowerLimit, Integer.MAX_VALUE), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else {
                                                logger.warn("unexpected start date company range node: " + dateRange.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(dateRangebq.build());
                                        }
                                        c++;
                                    }
                                    if (!shouldNode.findPath("user.tags.current_title_start_date").isMissingNode()) {
                                        BooleanQuery.Builder dateRangebq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode dateRange : shouldNode) {
                                            JsonNode dr = dateRange.get("range").get("user.tags.current_title_start_date");
                                            String fromDate = dr.get("from").isNull() ? "" : dr.get("from").asText();
                                            String toDate = dr.get("to").isNull() ? "" : dr.get("to").asText();
                                            int lowerLimit = getFirstNum(toDate);
                                            int upperLimit = getFirstNum(fromDate);
                                            if (lowerLimit >= 0 & upperLimit > lowerLimit) {
                                                dateRangebq.add(IntPoint.newRangeQuery("mcr", lowerLimit * 12, upperLimit * 12), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else if (lowerLimit > 0 & upperLimit == 0) {
                                                dateRangebq.add(IntPoint.newRangeQuery("mcr", lowerLimit * 12, Integer.MAX_VALUE), BooleanClause.Occur.SHOULD);
                                                mcounter++;
                                            } else {
                                                logger.warn("unexpected start date role range node: " + dateRange.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(dateRangebq.build());
                                        }
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.publication.citations").isMissingNode()) {
                                        return null;
                                    }
                                    if (!bNode.get("should").findPath("user.publication.i10_index").isMissingNode()) {
                                        return null;
                                    }
                                    if (!bNode.get("should").findPath("user.normed_skills").isMissingNode()) {
                                        // skip this, titles and skills have been embedded
                                        c++;
                                    }
                                    if (c == 0) {
                                        logger.warn("unexpected should filter: " + bNode.toString());
                                    } else if (c > 1) {
                                        logger.warn("unknown clause in should filter: " + bNode.toString());
                                    }
                                    count ++;
                                }
                                if (bNode.hasNonNull("must_not")) {
                                    JsonNode mnNode = bNode.get("must_not");
                                    int c = 0;
                                    if (!mnNode.findPath("user.user_id").isMissingNode()) {
                                        JsonNode uidArray = mnNode.findPath("user.user_id");
                                        for (JsonNode uid : uidArray) {
                                            excludebq.add(new TermQuery(new Term("uid", uid.asText())), BooleanClause.Occur.MUST_NOT);
                                            excludeCount++;
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.current_experience.companies").isMissingNode() ||
                                        !mnNode.findPath("user.current_experience.titles").isMissingNode() ||
                                        !mnNode.findPath("user.current_experience.normed_titles").isMissingNode()) {
                                        for (JsonNode currentPosition : mnNode) {
                                            if (!currentPosition.findPath("user.current_experience.companies").isMissingNode()) {
                                                String companyName = currentPosition.findPath("user.current_experience.companies").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("cc", companyName), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else if (!currentPosition.findPath("user.current_experience.titles").isMissingNode()) {
                                                String companyTitle = currentPosition.findPath("user.current_experience.titles").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("tc", companyTitle), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else if (!currentPosition.findPath("user.current_experience.normed_titles").isMissingNode()) {
                                                String companyNormedTitle = currentPosition.findPath("user.current_experience.normed_titles").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("ntc", companyNormedTitle), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else {
                                                logger.warn("unexpected must not currentPosition phrase match: " + currentPosition.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.past_experience.companies").isMissingNode() ||
                                        !mnNode.findPath("user.past_experience.titles").isMissingNode() ||
                                        !mnNode.findPath("user.past_experience.normed_titles").isMissingNode()) {
                                        for (JsonNode pastPosition : mnNode) {
                                            if (!pastPosition.findPath("user.past_experience.companies").isMissingNode()) {
                                                String companyName = pastPosition.findPath("user.past_experience.companies").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("cp", companyName), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else {
                                                logger.warn("unexpected must not pastPosition phrase match: " + pastPosition.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.current_experience.company_id").isMissingNode()) {
                                        if (mnNode.size() != 1) {
                                            logger.warn("unexpected must not currentCompanyIds term match: " + mnNode.toString());
                                        }
                                        if (!mnNode.findPath("user.current_experience.company_id").isMissingNode()) {
                                            for (JsonNode cic : mnNode.findPath("user.current_experience.company_id")) {
                                                excludebq.add(new TermQuery(new Term("cic", cic.asText())), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.past_experience.company_id").isMissingNode()) {
                                        if (mnNode.size() != 1) {
                                            logger.warn("unexpected must not pastCompanyIds term match: " + mnNode.toString());
                                        }
                                        if (!mnNode.findPath("user.past_experience.company_id").isMissingNode()) {
                                            for (JsonNode cip : mnNode.findPath("user.past_experience.company_id")) {
                                                excludebq.add(new TermQuery(new Term("cip", cip.asText())), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.current_experience.industries.keyword").isMissingNode()) {
                                        for(JsonNode industry : mnNode) {
                                            String industryTag = industry.get("term").get("user.current_experience.industries.keyword").get("value").asText();
                                            excludebq.add(new TermQuery(new Term("industry", industryTag.toLowerCase())), BooleanClause.Occur.MUST_NOT);
                                            excludeCount++;
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.reviewed_skills").isMissingNode() ||
                                        !mnNode.findPath("user.normed_skills").isMissingNode()) {
                                        for (JsonNode skillNode : mnNode) {
                                            if (!skillNode.findPath("user.reviewed_skills").isMissingNode()) {
                                                String rskill = skillNode.findPath("user.reviewed_skills").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("rs", rskill), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else if (!skillNode.findPath("user.normed_skills").isMissingNode()) {
                                                String nskill = skillNode.findPath("user.normed_skills").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("ns", nskill), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            } else {
                                                logger.warn("unexpected must not skill phrase match: " + skillNode.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.education.schools").isMissingNode()) {
                                        for (JsonNode education : mnNode) {
                                            if (!education.findPath("user.education.schools").isMissingNode()) {
                                                String schoolName = education.findPath("user.education.schools").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("eduSN", schoolName), BooleanClause.Occur.MUST_NOT);
                                            } else {
                                                logger.warn("unexpected must not school name phrase match: " + education.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.education.education_id").isMissingNode()) {
                                        for (JsonNode education : mnNode) {
                                            if (!education.findPath("user.education.education_id").isMissingNode()) {
                                                for (JsonNode eid : education.findPath("user.education.education_id")) {
                                                    excludebq.add(new TermQuery(new Term("eduSI", eid.asText())), BooleanClause.Occur.MUST_NOT);
                                                }
                                            } else {
                                                logger.warn("unexpected must not school id match: " + education.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!mnNode.findPath("user.global_search").isMissingNode()) {
                                        for (JsonNode query : mnNode) {
                                            if (!query.findPath("user.global_search").isMissingNode()) {
                                                String queryString = query.findPath("user.global_search").get("query").asText();
                                                excludebq.add(queryBuilder.createPhraseQuery("compound", queryString), BooleanClause.Occur.MUST_NOT);
                                            } else {
                                                logger.warn("unexpected must not global phrase match: " + query.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (c == 0) {
                                        logger.warn("unexpected must_not filter: " + bNode.toString());
                                    } else if (c > 1) {
                                        logger.warn("unknown clause in must_not filter: " + bNode.toString());
                                    }
                                    count ++;
                                }

                                if (bNode.hasNonNull("filter")) {
                                    JsonNode fNode = bNode.get("filter");
                                    int c = 0;
                                    if (!fNode.findPath("user.education.education_level.keyword").isMissingNode() ||
                                        !fNode.findPath("user.tags.business_administration_level").isMissingNode()) {
                                        for (JsonNode bfNode : fNode) {
                                            BooleanQuery.Builder eduLevelbq = new BooleanQuery.Builder();
                                            int mcounter = 0;
                                            JsonNode fbNode = bfNode.get("bool");
                                            if (fbNode.hasNonNull("should")) {
                                                for (JsonNode fbsNode : fbNode.get("should")) {
                                                    if (!fbsNode.findPath("user.education.education_level.keyword").isMissingNode()) {
                                                        for (JsonNode eduLevel : fbsNode.get("terms").get("user.education.education_level.keyword")) {
                                                            eduLevelbq.add(new TermQuery(new Term("eduLevel", eduLevel.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                                            mcounter++;
                                                        }
                                                    } else if (!fbsNode.findPath("user.tags.business_administration_level.keyword").isMissingNode()) {
                                                        for (JsonNode eduBAL : fbsNode.get("terms").get("user.tags.business_administration_level.keyword")) {
                                                            eduLevelbq.add(new TermQuery(new Term("eduBALK", eduBAL.asText().toLowerCase())), BooleanClause.Occur.SHOULD);
                                                            mcounter++;
                                                        }
                                                    } else if (!fbsNode.findPath("user.education.degrees").isMissingNode()) {
                                                        String degreeStr = fbsNode.get("match_phrase").get("user.education.degrees").get("query").asText();
                                                        eduLevelbq.add(queryBuilder.createPhraseQuery("eduDegree", degreeStr), BooleanClause.Occur.SHOULD);
                                                        mcounter++;
                                                    } else {
                                                        logger.warn("unexpected field in filter bool should node: " + fbsNode.toString());
                                                    }
                                                }
                                            } else if (fbNode.hasNonNull("must_not")) {
                                                for (JsonNode fbsNode : fbNode.get("must_not")) {
                                                    if (!fbsNode.findPath("user.tags.business_administration_level").isMissingNode()) {
                                                        String eduBAL = fbsNode.findPath("user.tags.business_administration_level").get("query").asText();
                                                        excludebq.add(queryBuilder.createPhraseQuery("eduBAL", eduBAL), BooleanClause.Occur.MUST_NOT);
                                                        excludeCount++;
                                                    } else {
                                                        logger.warn("unexpected field in filter bool must_not node: " + fbsNode.toString());
                                                    }
                                                }
                                            } else {
                                                logger.warn("unexpected field in education_level filter node: " + fbNode.toString());
                                            }
                                            if (mcounter > 0) {
                                                ret.add(eduLevelbq.build());
                                            }
                                        }
                                        c++;
                                    }
                                    if (!fNode.findPath("user.tags.gender").isMissingNode() ||
                                        !fNode.findPath("user.tags.race").isMissingNode() ||
                                        !fNode.findPath("user.tags.veteran").isMissingNode()) {
                                        BooleanQuery.Builder diversitybq = new BooleanQuery.Builder();
                                        int mcounter = 0;
                                        for(JsonNode diversity : fNode) {
                                            if (!diversity.findPath("user.tags.gender").isMissingNode()) {
                                                if (diversity.findPath("user.tags.gender").get("value").asText().equals("female")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divWoman", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.race").isMissingNode()) {
                                                String race = diversity.findPath("user.tags.race").get("value").asText();
                                                if (race.equals("african-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divBlack", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else if (race.equals("hispanic")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divHispanic", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else if (race.equals("native-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divNative", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else if (race.equals("asian-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divAsian", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.veteran").isMissingNode()) {
                                                if (diversity.findPath("user.tags.veteran").get("value").asBoolean()) {
                                                    diversitybq.add(IntPoint.newExactQuery("divVeteran", 1), BooleanClause.Occur.MUST);
                                                    mcounter++;
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else {
                                                logger.warn("unexpected diversity field!!!" + diversity.toString());
                                            }
                                        }
                                        if (mcounter > 0) {
                                            ret.add(diversitybq.build());
                                        }
                                        c++;
                                    }
                                    if (c == 0) {
                                        logger.warn("unexpected bool filter node: " + bNode.toString());
                                    } else if (c > 1) {
                                        logger.warn("unknown clause in filter node: " + bNode.toString());
                                    }
                                    count++;
                                }

                                Iterator<String> fn = bNode.fieldNames();
                                while (fn.hasNext()) {
                                    String n = fn.next();
                                    if (!n.equals("must_not") && !n.equals("should") && !n.equals("filter")
                                            && !n.equals("adjust_pure_negative") && !n.equals("boost")) {
                                        logger.warn("unknown field in filter bool node: " + n + "|" + bNode.toString());
                                    }
                                }
                                if (count > 1) {
                                    logger.warn("multi clauses in bool filter: " + bNode.toString());
                                }
                            } else {
                                logger.warn("should not happen, unexpected bool filter: " + bNode.toString());
                            }
                        } else {
                            logger.warn("unexpected single bool node: " + node.toString());
                        }
                    }
                    if (eduGradYearFrom > -1 && eduGradYearTo > -1) {
                        ret.add(IntPoint.newRangeQuery("eduGradYear", eduGradYearFrom, eduGradYearTo));
                    } else if (eduGradYearFrom == -1 && eduGradYearTo > -1) {
                        ret.add(IntPoint.newRangeQuery("eduGradYear", Integer.MIN_VALUE, eduGradYearTo));
                    } else if (eduGradYearFrom > -1 && eduGradYearTo == -1) {
                        ret.add(IntPoint.newRangeQuery("eduGradYear", eduGradYearFrom, Integer.MAX_VALUE));
                    }
                } else {
                    logger.warn("unexpected filter node: " + filterNode.toString());
                }
            }

            if (!boolNode.isNull() && boolNode.has("must_not")) {
                JsonNode mnNode = boolNode.get("must_not");
                if (!mnNode.isNull() && !mnNode.isEmpty() && mnNode.isArray()) {
                    for (JsonNode mn : mnNode) {
                        if (!mn.findPath("user.tags.visa_sponsorship").isMissingNode()) {
                            if (mn.get("term").get("user.tags.visa_sponsorship").get("value").asBoolean()) {
                                excludebq.add(IntPoint.newExactQuery("needSponsorship", 1), BooleanClause.Occur.MUST_NOT);
                                excludeCount++;
                            }
                        } else if (!mn.findPath("user.tags.has_personal_email").isMissingNode()) {
                            if (mn.get("term").get("user.tags.has_personal_email").get("value").asBoolean()) {
                                excludebq.add(IntPoint.newExactQuery("hasPersonalEmail", 1), BooleanClause.Occur.MUST_NOT);
                                excludeCount++;
                            }
                        } else if (!mn.findPath("user.location.location_value").isMissingNode() ||
                                   !mn.findPath("user.location.continent.keyword").isMissingNode() ||
                                   !mn.findPath("user.location.country.keyword").isMissingNode() ||
                                   !mn.findPath("user.location.state.keyword").isMissingNode()) {
                            for (JsonNode loc : mn.get("bool").get("should")) {
                                if (!loc.findPath("user.location.location_value").isMissingNode()) {
                                    excludebq.add(queryBuilder.createPhraseQuery("loc", loc.get("match_phrase")
                                            .get("user.location.location_value").get("query").asText()), BooleanClause.Occur.MUST_NOT);
                                    excludeCount++;
                                } else if (!loc.findPath("user.location.continent.keyword").isMissingNode() ||
                                           !loc.findPath("user.location.country.keyword").isMissingNode() ||
                                           !loc.findPath("user.location.state.keyword").isMissingNode()) {
                                    BooleanQuery.Builder locbq = new BooleanQuery.Builder();
                                    JsonNode blocNode = loc.get("bool");
                                    if (blocNode.hasNonNull("must")) {
                                        for (JsonNode locPart : blocNode.get("must")) {
                                            if (!locPart.findPath("user.location.continent.keyword").isMissingNode()) {
                                                locbq.add(new TermQuery(new Term("continent", locPart.get("term")
                                                        .get("user.location.continent.keyword").get("value").asText().toLowerCase())), BooleanClause.Occur.MUST);
                                            } else if (!locPart.findPath("user.location.country.keyword").isMissingNode()) {
                                                locbq.add(new TermQuery(new Term("country", locPart.get("term")
                                                        .get("user.location.country.keyword").get("value").asText().toLowerCase())), BooleanClause.Occur.MUST);
                                            } else if (!locPart.findPath("user.location.state.keyword").isMissingNode()) {
                                                locbq.add(new TermQuery(new Term("state", locPart.get("term")
                                                        .get("user.location.state.keyword").get("value").asText().toLowerCase())), BooleanClause.Occur.MUST);
                                            } else if (!locPart.findPath("user.location.geo_coordinates").isMissingNode()) {
                                                JsonNode latlon = locPart.get("geo_distance").get("user.location.geo_coordinates");
                                                locbq.add(LatLonPoint.newDistanceQuery("distance", latlon.get(1).asDouble(), latlon.get(0).asDouble(),
                                                        locPart.get("geo_distance").get("distance").asInt()), BooleanClause.Occur.MUST);
                                            } else {
                                                logger.warn("unexpected field in must not loc node: " + locPart.toString());
                                            }
                                        }
                                    }
                                    if (blocNode.hasNonNull("must_not")) {
                                        for (JsonNode locPart : blocNode.get("must_not")) {
                                            if (!locPart.findPath("user.location.type.keyword").isMissingNode()) {
                                                locbq.add(new TermQuery(new Term("locType", locPart.get("term")
                                                        .get("user.location.type.keyword").get("value").asText().toLowerCase())), BooleanClause.Occur.MUST);
                                            } else {
                                                logger.warn("unexpected field in must not must not loc node: " + locPart.toString());
                                            }
                                        }
                                    }
                                    excludebq.add(locbq.build(), BooleanClause.Occur.MUST_NOT);
                                    excludeCount++;
                                } else {
                                    logger.warn("unknown field in must_not loc node: " + loc.toString());
                                }
                            }
                        } else {
                            logger.warn("unknown field in must_not node: " + mn.toString());
                        }
                    }
                } else {
                    logger.warn("unexpected must_not node: " + mnNode.toString());
                }
            }

            if (!boolNode.isNull() && boolNode.has("should")) {
                JsonNode sArray = boolNode.get("should");
                BooleanQuery.Builder shouldbq = new BooleanQuery.Builder();
                int mcounter = 0;
                if (!sArray.isNull() && !sArray.isEmpty() && sArray.isArray()) {
                    for (JsonNode sNode : sArray) {
                        if (!sNode.findPath("user.title_skill_search").isMissingNode()) {
                            // skip this, titles and skills have been embedded
                        } else if (!sNode.findPath("user.tags.has_contact").isMissingNode()) {
                            //TODO: boostQuery may slow
                            shouldbq.add(new BoostQuery(IntPoint.newExactQuery("hasContact", 1), sNode.get("term")
                                    .get("user.tags.has_contact").get("boost").asInt()), BooleanClause.Occur.SHOULD);
                            mcounter++;
                        } else if (!sNode.findPath("user.tags.has_personal_email").isMissingNode()) {
                            //TODO: boostQuery may slow
                            shouldbq.add(new BoostQuery(IntPoint.newExactQuery("hasPersonalEmail", 1), sNode.get("term")
                                    .get("user.tags.has_personal_email").get("boost").asInt()), BooleanClause.Occur.SHOULD);
                            mcounter++;
                        }else if (!sNode.findPath("user.tags.race_confidence").isMissingNode()) {
                            // ignore this part
                        } else if (sNode.has("exists") && sNode.get("exists").get("field").asText()
                                .equals("user.healthcare.npi_number") && getFieldsCount(sNode.get("exists")) == 2) {
                            // ignore this part
                        } else if (sNode.has("exists") && sNode.get("exists").get("field").asText()
                                .equals("user.social_links.medical_board_url") && getFieldsCount(sNode.get("exists")) == 2) {
                            // ignore this part
                        } else if (sNode.has("exists") && sNode.get("exists").get("field").asText()
                                .equals("user.social_links.doximity_url") && getFieldsCount(sNode.get("exists")) == 2) {
                            // ignore this part
                        } else if (sNode.has("exists") && sNode.get("exists").get("field").asText()
                                .equals("user.social_links.ratemd_url") && getFieldsCount(sNode.get("exists")) == 2) {
                            // ignore this part
                        } else if (sNode.has("exists") && sNode.get("exists").get("field").asText()
                                .equals("user.social_links.healthgrades_url") && getFieldsCount(sNode.get("exists")) == 2) {
                            // ignore this part
                        } else {
                            logger.warn("unknown should node: " + sNode.toString());
                        }
                    }
                } else {
                    logger.warn("unexpected should node: " + sArray.toString());
                }
                if (mcounter > 0) {
                    ret.add(shouldbq.build());
                }
            }

            Iterator<String> fn = boolNode.fieldNames();
            while (fn.hasNext()) {
                String n = fn.next();
                if (!n.equals("must") && !n.equals("filter") && !n.equals("must_not") && !n.equals("should")
                        && !n.equals("adjust_pure_negative") && !n.equals("boost")) {
                    logger.warn("unknown field in bool node: " + n + "|" + boolNode.toString());
                }
            }
        } else {
            logger.warn("strange es query structure: " + esQuery.toString());
        }

        if(excludeCount > 0) {
            ret.add(excludebq.build());
        }

        return ret;
    }

    private static int getFieldsCount(JsonNode node) {
        if (node == null || node.isEmpty()) {
            return 0;
        }
        Iterator<String> it = node.fieldNames();
        int ret = 0;
        while (it.hasNext()) {
            ret += (it.next() != null ? 1 : 0);
        }
        return ret;
    }

    private static int getFirstNum(String str) {
        if (str == null || str.length() == 0) {
            return 0;
        }

        boolean metNum = false;
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < str.length(); i++){
            if(str.charAt(i) >= 48 && str.charAt(i) <= 57){
                sb.append(str.charAt(i));
                metNum = true;
            } else {
                if (metNum) {
                    break;
                }
            }
        }
        int ret = 0;
        try {
            ret = Integer.parseInt(sb.toString());
        } catch(Exception e) {
            logger.warn("fail to parse ");
        }
        return ret;
    }

    private static String[] split2Array(String str) {
        if (str == null || str.length() == 0) {
            return new String[0];
        }

        return str.split("\\W+");
    }

    public static JsonNode postForEntityWithHeader(String content) {
        String url = "http://elasticsearch.db.testhtm/api/v3/user/generate_es_query";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity(content, headers);
        RestTemplate restTemplate = new RestTemplate();
        String response = restTemplate.postForObject(url, request, String.class);
//        logger.info("es query: " + response);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(response);
        } catch(Exception e) {
            logger.warn(response, e);
        }
        return null;
    }

    public static JsonNode extractESQuery(JsonNode query){
        JsonNode esQuery=query.get("esQuery");
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(esQuery.asText());
        } catch(Exception e) {
            logger.warn("fail to extarct es query", e);
        }
        return null;
    }

//    public static void main(String[] args) {
//        ObjectMapper objectMapper = new ObjectMapper();
//        String line = null;
//        try {
//            File file = new File("/Users/jetyang/esquery");
//            if (!file.isDirectory()) {
//                long t1 = System.currentTimeMillis();
//                BufferedReader br = new BufferedReader(new FileReader(file));
//
//                int k = 0;
//                int cc = 0;
//                while ((line = br.readLine()) != null) {
//                    JsonNode query = objectMapper.readTree(line);
//                    List<Query> list = QueryConvertor.convertESQuery(query);
//                    if (list == null) {
//                        cc++;
//                    }
//                    k++;
//
//                    if (k % 11900 == 0) {
//                        logger.info(line);
//                        if (list != null) {
//                            BooleanQuery.Builder builder = new BooleanQuery.Builder();
//                            for (Query q : list) {
//                                builder.add(q, BooleanClause.Occur.FILTER);
//                            }
//                            logger.info(list.size() +  "|" + builder.build().toString());
//                        }
//                    }
//                }
//                long t2 = System.currentTimeMillis();
//                logger.info(k + " queries parsed, " + cc + " returned null, time cost: " + (t2 - t1) + " ms");
//            }
//        } catch (FileNotFoundException e) {
//            logger.error("file not found", e);
//        } catch (IOException e) {
//            logger.error("can not read file", e);
//        } catch (Exception e) {
//            logger.error("can not parse json string: " + line, e);
//        }
//    }
}
