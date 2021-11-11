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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.*;

public class QueryConvertor {

    private static final Logger logger = LoggerFactory.getLogger(JedisUtils.class);

    public static List<Query> convertESQuery(JsonNode esQuery) {
        List<Query> ret = new ArrayList<>();
        BooleanQuery.Builder excludebq = new BooleanQuery.Builder();
        MatchAllDocsQuery madq = new MatchAllDocsQuery();
        excludebq.add(madq, BooleanClause.Occur.SHOULD);
        int excludeCount = 0;

        if (esQuery != null && !esQuery.isNull() && esQuery.has("bool")) {
            JsonNode boolNode = esQuery.get("bool");
            if (!boolNode.isNull() && boolNode.has("filter")) {
                JsonNode filterNode = boolNode.get("filter");
                if (!filterNode.isNull() && !filterNode.isEmpty() && filterNode.isArray()) {
                    for (JsonNode node : filterNode) {
                        if (!node.isNull() && node.has("term")) {
                            JsonNode termNode = node.get("term");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                continue;
                            } else if (termNode.has("user.tags.has_personal_email") && getFieldsCount(termNode) == 1) {
                                continue;
                            } else if (termNode.has("user.location.country.keyword") && getFieldsCount(termNode) == 1) {
                                ret.add(new TermQuery(new Term("nation", termNode.get("user.location.country.keyword").get("value").asText())));
                            } else {
                                logger.info("unexpected term query:" + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("terms")) {
                            JsonNode termNode = node.get("terms");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                continue;
                            } else if (termNode.has("user.tags.seniority") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                //TODO: directly return
//                                return null;
                                continue;
                            } else if (termNode.has("user.tags.security_clearance.keyword") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                //TODO: directly return
//                                return null;
                                continue;
                            } else if (termNode.has("user.current_experience.normed_titles") && termNode.has("boost") && getFieldsCount(termNode) == 2) {
                                // ignore normalized titles
                                continue;
                            } else {
                                logger.info("unexpected terms query:" + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("exists")) {
                            JsonNode termNode = node.get("exists");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                continue;
                            } else if (termNode.has("field") && termNode.get("field").asText().equals("user.info_tech") && getFieldsCount(termNode) == 2) {
                                //TODO: info_tech need to be handled
                                continue;
                            } else {
                                logger.info("unexpected terms query:" + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("range")) {
                            JsonNode termNode = node.get("range");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                continue;
                            } else if (termNode.has("user.education.grad_year") && getFieldsCount(termNode) == 1) {
                                //TODO: directly return
//                                return null;
                                continue;
                            } else {
                                logger.info("unexpected education grad year filter:" + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("query_string")) {
                            JsonNode termNode = node.get("query_string");
                            if (termNode.isNull() || termNode.isEmpty()) {
                                continue;
                            } else if (termNode.has("query") && termNode.get("default_field").asText().equals("user.global_search")) {
                                Analyzer analyzer = new StandardAnalyzer();
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
                                    logger.error("", e);
                                    continue;
                                }
                            } else {
                                logger.info("unexpected query string filter:" + termNode.toString());
                            }
                        } else if (!node.isNull() && node.has("bool")) {
                            JsonNode bNode = node.get("bool");
                            if (!bNode.isNull() && !bNode.isEmpty()) {
                                int count = 0;
                                if (bNode.has("should")) {
                                    int c = 0;
                                    if (!bNode.get("should").findPath("user.client_id").isMissingNode() ||
                                        !bNode.get("should").findPath("user.sourced_client_ids").isMissingNode() ||
                                        !bNode.get("should").findPath("user.sourcing_channels.keyword").isMissingNode()) {
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.title_skill_search").isMissingNode()) {
//                                        JsonNode titleSkills = bNode.get("should");
//                                        BooleanQuery.Builder tsbq = new BooleanQuery.Builder();
//                                        for(JsonNode ts : titleSkills) {
//                                            tsbq.add(new PhraseQuery("compound", split2Array(ts.get("match_phrase").get("user.title_skill_search").get("query").asText())), BooleanClause.Occur.SHOULD);
//                                        }
//                                        ret.add(tsbq.build();
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.location.location_value").isMissingNode() ||
                                               !bNode.get("should").findPath("user.location.continent.keyword").isMissingNode() ||
                                               !bNode.get("should").findPath("geo_distance").isMissingNode()) {
                                        JsonNode locs = bNode.get("should");
                                        BooleanQuery.Builder lbq = new BooleanQuery.Builder();
                                        Set<String> locValueSet = new HashSet<>();
                                        Set<String> locObjectSet = new HashSet<>();
                                        for (JsonNode loc : locs) {
                                            if (!loc.findPath("user.location.location_value").isMissingNode()) {

                                                String locValue = loc.get("match_phrase").get("user.location.location_value").get("query").asText().toLowerCase();
                                                if (!locValueSet.contains(locValue)) {
                                                    lbq.add(new PhraseQuery("compound", split2Array(locValue)), BooleanClause.Occur.SHOULD);
                                                    ret.add(new PhraseQuery("compound", split2Array(locValue)));
                                                    locValueSet.add(locValue);
                                                }

                                            } else if (loc.has("bool")) {
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
                                                            lmbq.add(new TermQuery(new Term("nation", country.toLowerCase())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.state.keyword").isMissingNode()) {
                                                            String state = must.get("term").get("user.location.state.keyword").get("value").asText();
                                                            lsb.append('+').append(state);
                                                            lmbq.add(new TermQuery(new Term("state", state.toLowerCase())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.city.keyword").isMissingNode()) {
                                                            String city = must.get("term").get("user.location.city.keyword").get("value").asText();
                                                            lsb.append('+').append(city);
                                                            lmbq.add(new TermQuery(new Term("city", city.toLowerCase())), BooleanClause.Occur.MUST);
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
                                                            //TODO: change continent to location type
//                                                            lmbq.add(new TermQuery(new Term("continent", locType)), BooleanClause.Occur.MUST_NOT);
                                                        } else {
                                                            logger.warn("unexpected geo field: " + mustnot.toString());
                                                        }
                                                    }
                                                }
                                                if (!locObjectSet.contains(lsb.toString())) {
                                                    lbq.add(lmbq.build(), BooleanClause.Occur.SHOULD);
                                                    ret.add(lmbq.build());
                                                    locObjectSet.add(lsb.toString());
                                                }
                                            } else {
                                                logger.warn("unexpected loc filter: " + loc.toString());
                                            }
                                        }
                                        ret.add(lbq.build());
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.current_experience.normed_titles").isMissingNode() ||
                                               !bNode.get("should").findPath("user.past_experience.titles").isMissingNode() ||
                                               !bNode.get("should").findPath("user.past_experience.normed_titles").isMissingNode()) {

//                                        JsonNode titles = bNode.get("should");
//                                        BooleanQuery.Builder tbq = new BooleanQuery.Builder();
//                                        for(JsonNode title : titles) {
//                                            if (!title.findPath("user.current_experience.titles").isMissingNode()) {
//                                                tbq.add(new PhraseQuery("cc", split2Array(title.get("match_phrase").get("user.current_experience.titles").get("query").asText())), BooleanClause.Occur.SHOULD);
//                                            } else if (!title.findPath("user.current_experience.normed_titles").isMissingNode()) {
//                                                // TODO: check field name if necessary
//                                                tbq.add(new PhraseQuery("cic", split2Array(title.get("match_phrase").get("user.current_experience.normed_titles").get("query").asText())), BooleanClause.Occur.SHOULD);
//                                            } else if (!title.findPath("user.past_experience.titles").isMissingNode()) {
//                                                tbq.add(new PhraseQuery("cp", split2Array(title.get("match_phrase").get("user.past_experience.titles").get("query").asText())), BooleanClause.Occur.SHOULD);
//                                            } else if (!title.findPath("user.past_experience.normed_titles").isMissingNode()) {
//                                                // TODO: check field name if necessary
//                                                tbq.add(new PhraseQuery("cip", split2Array(title.get("match_phrase").get("user.past_experience.normed_titles").get("query").asText())), BooleanClause.Occur.SHOULD);
//                                            } else {
//                                                logger.warn("sth unexpected!!! " + title.toString());
//                                            }
//                                        }
//                                        ret.add(tbq.build());

                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.tags.experience_tag").isMissingNode()) {
                                        JsonNode hf = bNode.get("should");
                                        if (hf.size() > 1) {
                                            logger.info("multi field in json:" + hf.toString());
                                        }
                                        BooleanQuery.Builder yoebq = new BooleanQuery.Builder();
                                        for(JsonNode h : hf) {
                                            JsonNode terms = h.get("terms");
                                            Iterator<String> tf = terms.fieldNames();
                                            while (tf.hasNext()) {
                                                String fieldName = tf.next();
                                                if (fieldName.equals("user.tags.experience_tag")) {
                                                    JsonNode yoes = terms.get("user.tags.experience_tag");
                                                    for (JsonNode yoe : yoes) {
                                                        yoebq.add(new TermQuery(new Term("yoe", yoe.asText())), BooleanClause.Occur.SHOULD);
                                                    }
                                                } else if (fieldName.equals("boost")) {
                                                    continue;
                                                } else {
                                                    logger.warn("sth unexpected!!! " + terms.toString());
                                                }
                                            }
                                        }
                                        ret.add(yoebq.build());
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.education.education_level.keyword").isMissingNode()) {
                                        JsonNode degrees = bNode.get("should");
                                        if (degrees.size() > 2) {
                                            logger.info("too many fields in degree json:" + degrees.toString());
                                        }
                                        BooleanQuery.Builder degreebq = new BooleanQuery.Builder();
                                        for(JsonNode degree : degrees) {
                                            JsonNode terms = degree.get("terms");
                                            Iterator<String> tf = terms.fieldNames();
                                            while (tf.hasNext()) {
                                                String fieldName = tf.next();
                                                if (fieldName.equals("user.education.education_level.keyword")) {
                                                    JsonNode ds = terms.get("user.education.education_level.keyword");
                                                    for (JsonNode d : ds) {
                                                        degreebq.add(new TermQuery(new Term("degree", d.asText())), BooleanClause.Occur.SHOULD);
                                                    }
                                                } else if (fieldName.equals("user.tags.business_administration_level.keyword")) {
                                                    continue;
                                                } else if (fieldName.equals("boost")) {
                                                    continue;
                                                } else {
                                                    logger.warn("sth unexpected!!! " + terms.toString());
                                                }
                                            }
                                        }
                                        ret.add(degreebq.build());
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.current_experience.company_id").isMissingNode() ||
                                        !bNode.get("should").findPath("user.past_experience.company_id").isMissingNode()) {
                                        JsonNode companyIds = bNode.get("should");
                                        BooleanQuery.Builder cibq = new BooleanQuery.Builder();
                                        if (!companyIds.findPath("user.current_experience.company_id").isMissingNode()) {
                                            for (JsonNode ci : companyIds.findPath("user.current_experience.company_id")) {
                                                cibq.add(new TermQuery(new Term("cic", ci.asText())), BooleanClause.Occur.SHOULD);
                                            }
                                        }
                                        if (!companyIds.findPath("user.past_experience.company_id").isMissingNode()) {
                                            for (JsonNode ci : companyIds.findPath("user.past_experience.company_id")) {
                                                cibq.add(new TermQuery(new Term("cip", ci.asText())), BooleanClause.Occur.SHOULD);
                                            }
                                        }
                                        if (companyIds.size() > 2) {
                                            logger.info("too many fields in company id json:" + companyIds.toString());
                                        }
                                        ret.add(cibq.build());
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.education.majors").isMissingNode()) {
                                        JsonNode majors = bNode.get("should");
                                        BooleanQuery.Builder majorbq = new BooleanQuery.Builder();
                                        for(JsonNode major : majors) {
                                            JsonNode terms = major.findPath("should");
                                            if (terms.size() > 2) {
                                                logger.info("too many fields in major json:" + terms.toString());
                                            }
                                            for (JsonNode maj : terms) {
                                                if (!maj.findPath("user.education.majors").isMissingNode()) {
                                                    //TODO: maybe not necessary
                                                    majorbq.add(new PhraseQuery("compound", split2Array(maj.get("match_phrase").get("user.education.majors").get("query").asText())), BooleanClause.Occur.SHOULD);
                                                } else if (!maj.findPath("user.education.degrees").isMissingNode()) {
                                                    continue;
                                                } else {
                                                    logger.warn("sth unexpected!!! " + terms.toString());
                                                }
                                            }
                                        }
                                        ret.add(majorbq.build());
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.tags.gender").isMissingNode() ||
                                        !bNode.get("should").findPath("user.tags.race").isMissingNode() ||
                                        !bNode.get("should").findPath("user.tags.veteran").isMissingNode()) {
                                        JsonNode diversities = bNode.get("should");
                                        BooleanQuery.Builder diversitybq = new BooleanQuery.Builder();
                                        for(JsonNode diversity : diversities) {
                                            if (!diversity.findPath("user.tags.gender").isMissingNode()) {
                                                if (diversity.findPath("user.tags.gender").get("value").asText().equals("female")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divWoman", 1), BooleanClause.Occur.SHOULD);
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.race").isMissingNode()) {
                                                String race = diversity.findPath("user.tags.race").get("value").asText();
                                                if (race.equals("african-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divBlack", 1), BooleanClause.Occur.SHOULD);
                                                } else if (race.equals("hispanic")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divHispanic", 1), BooleanClause.Occur.SHOULD);
                                                } else if (race.equals("native-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divNative", 1), BooleanClause.Occur.SHOULD);
                                                } else if (race.equals("asian-american")) {
                                                    diversitybq.add(IntPoint.newExactQuery("divAsian", 1), BooleanClause.Occur.SHOULD);
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else if (!diversity.findPath("user.tags.veteran").isMissingNode()) {
                                                if (diversity.findPath("user.tags.veteran").get("value").asBoolean()) {
                                                    diversitybq.add(IntPoint.newExactQuery("divVeteran", 1), BooleanClause.Occur.SHOULD);
                                                } else {
                                                    logger.warn("sth strange!!!" + diversity.toString());
                                                }
                                            } else {
                                                logger.warn("unexpected diversity field!!!" + diversity.toString());
                                            }
                                        }
                                        ret.add(diversitybq.build());
                                        c++;
                                    }
                                    if (c == 0) {
                                        logger.info("unexpected should filter:" + bNode.toString());
                                    } else if (c > 1) {
                                        logger.info("unknown clause in should filter:" + bNode.toString());
                                    }
                                    count ++;
                                }
                                if (bNode.has("must_not")) {
                                    int c = 0;
                                    if (!bNode.get("must_not").findPath("user.user_id").isMissingNode()) {
                                        JsonNode uidArray = bNode.get("must_not").findPath("user.user_id");
                                        for (JsonNode uid : uidArray) {
                                            excludebq.add(new TermQuery(new Term("uid", uid.asText())), BooleanClause.Occur.MUST_NOT);
                                            excludeCount++;
                                        }
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.current_experience.companies").isMissingNode()) {
                                        JsonNode currentPositions = bNode.get("must_not");
                                        int k = currentPositions.size();
                                        if (!currentPositions.findPath("user.current_experience.companies").isMissingNode()) {
                                            excludebq.add(new PhraseQuery("compound", split2Array(currentPositions.findPath("user.current_experience.companies").get("query").asText())), BooleanClause.Occur.MUST_NOT);
                                            excludeCount++;
                                            k --;
                                        }
                                        //TODO: correct field name - current titles
                                        if (!currentPositions.findPath("user.current_experience.titles").isMissingNode()) {
//                                            excludeCompanybq.add(new PhraseQuery("compound", split2Array(currentPositions.findPath("user.current_experience.titles").get("query").asText())), BooleanClause.Occur.MUST_NOT);
                                            k--;
                                        }
                                        if (k != 0) {
                                            logger.info("unexpected must not phrase match: " + currentPositions.toString());
                                        }
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.current_experience.company_id").isMissingNode()) {
                                        JsonNode currentCompanyIds = bNode.get("must_not");
                                        int k = currentCompanyIds.size();
                                        if (!currentCompanyIds.findPath("user.current_experience.company_id").isMissingNode()) {
                                            for (JsonNode cic : currentCompanyIds.findPath("user.current_experience.company_id")) {
                                                excludebq.add(new TermQuery(new Term("cic", cic.asText())), BooleanClause.Occur.MUST_NOT);
                                                excludeCount++;
                                            }
                                            k --;
                                        }
                                        if (k != 0) {
                                            logger.info("unexpected must not term match: " + currentCompanyIds.toString());
                                        }
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.education.schools").isMissingNode()) {
                                        //TODO: directly return
//                                        return null;
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.education.education_id").isMissingNode()) {
                                        // no need to handle excluded education school ids
                                        //TODO: directly return
//                                        return null;
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.tags.business_administration_level").isMissingNode()) {
                                        //TODO: handle this field later
                                        for (JsonNode bal : bNode.get("must_not")) {
                                            if (bal.findPath("user.tags.business_administration_level").isMissingNode()) {
                                                logger.warn("unexpected business administration level node: " + bal.toString());
                                            }
                                        }
                                        c++;
                                    }
                                    if (c == 0) {
                                        logger.info("unexpected must_not filter:" + bNode.toString());
                                    } else if (c > 1) {
                                        logger.info("unknown clause in must_not filter:" + bNode.toString());
                                    }
                                    count ++;
                                }
                                if (bNode.has("must")) {
                                    logger.info("must clause in bool filter:" + bNode.toString());
                                    count ++;
                                }
                                if (count > 1) {
                                    logger.info("multi clauses in bool filter:" + bNode.toString());
                                }
                            } else {
                                logger.warn("should not happen, unexpected bool filter:" + bNode.toString());
                            }
                        } else {
                            logger.warn("unexpected single bool node:" + node.toString());
                        }
                    }
                } else {
                    logger.warn("strange filter node:" + filterNode.toString());
                }

            }
            if (!boolNode.isNull() && boolNode.has("should")) {
                JsonNode filterNode = boolNode.get("should");
                BooleanQuery.Builder tsbq = new BooleanQuery.Builder();
                if (!filterNode.isNull() && !filterNode.isEmpty() && filterNode.isArray()) {
                    for (JsonNode match : filterNode) {
                        if (!match.findPath("user.title_skill_search").isMissingNode()) {

                            //TODO: handle boost later
//                            tsbq.add(new PhraseQuery("cc", split2Array(match.get("match_phrase").get("user.title_skill_search").get("query").asText())), BooleanClause.Occur.SHOULD);

                        } else if (!match.findPath("user.tags.has_contact").isMissingNode() || !match.findPath("user.tags.has_personal_email").isMissingNode()) {
                        } else {
                            logger.warn("unknown should node: " + filterNode.toString());
                        }
                    }
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
                            continue;
                        } else {
                            logger.warn("unknown must_not node: " + mn.toString());
                        }
                    }
                }
            }
            Iterator<String> fn = boolNode.fieldNames();
            while (fn.hasNext()) {
                String n = fn.next();
                if (!n.equals("filter") && !n.equals("must_not") && !n.equals("should") && !n.equals("adjust_pure_negative") && !n.equals("boost")) {
                    logger.warn("unknown field in bool node: " + n);
                }

            }
        } else {
            logger.warn("strange es query:" + esQuery.toString());
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
}
