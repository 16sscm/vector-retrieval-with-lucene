package com.hiretual.search.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.Iterator;

public class QueryConvertor {

    private static final Logger logger = LoggerFactory.getLogger(JedisUtils.class);

    public static BooleanQuery convertESQuery(JsonNode esQuery) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();

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
                            } else {
                                logger.info("unexpected term query:" + termNode.toString());
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
                                        continue;
                                    }
                                    if (!bNode.get("should").findPath("user.title_skill_search").isMissingNode()) {
                                        JsonNode titleSkills = bNode.get("should");
                                        BooleanQuery.Builder tsbq = new BooleanQuery.Builder();
                                        for(JsonNode ts : titleSkills) {
                                            tsbq.add(new PhraseQuery("compound", split2Array(ts.get("match_phrase").get("user.title_skill_search").get("query").asText())), BooleanClause.Occur.SHOULD);
                                        }
                                        bq.add(tsbq.build(), BooleanClause.Occur.MUST);
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.location.location_value").isMissingNode() ||
                                               !bNode.get("should").findPath("user.location.continent.keyword").isMissingNode() ||
                                               !bNode.get("should").findPath("geo_distance").isMissingNode()) {
                                        JsonNode locs = bNode.get("should");
                                        BooleanQuery.Builder lbq = new BooleanQuery.Builder();
                                        for (JsonNode loc : locs) {
                                            if (!loc.findPath("user.location.location_value").isMissingNode()) {
                                                lbq.add(new PhraseQuery("compound", split2Array(loc.get("match_phrase").get("user.location.location_value").get("query").asText())), BooleanClause.Occur.SHOULD);
                                            } else if (loc.has("bool")) {
                                                JsonNode lboolNode = loc.get("bool");
                                                BooleanQuery.Builder lmbq = new BooleanQuery.Builder();
                                                if (lboolNode.has("must")) {
                                                    for (JsonNode must : lboolNode.get("must")) {
                                                        if (!must.findPath("user.location.continent.keyword").isMissingNode()) {
                                                            lmbq.add(new TermQuery(new Term("continent", must.get("term").get("user.location.continent.keyword").get("value").asText())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.country.keyword").isMissingNode()) {
                                                            lmbq.add(new TermQuery(new Term("nation", must.get("term").get("user.location.country.keyword").get("value").asText())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.state.keyword").isMissingNode()) {
                                                            lmbq.add(new TermQuery(new Term("state", must.get("term").get("user.location.state.keyword").get("value").asText())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.city.keyword").isMissingNode()) {
                                                            lmbq.add(new TermQuery(new Term("city", must.get("term").get("user.location.city.keyword").get("value").asText())), BooleanClause.Occur.MUST);
                                                        } else if (!must.findPath("user.location.geo_coordinates").isMissingNode()) {
                                                            JsonNode latlon = must.get("geo_distance").get("user.location.geo_coordinates");
                                                            lmbq.add(LatLonPoint.newDistanceQuery("distance", latlon.get(1).asDouble(), latlon.get(0).asDouble(), must.get("geo_distance").get("distance").asInt()), BooleanClause.Occur.MUST);
                                                        } else {
                                                            logger.warn("unexpected geo field: " + must.toString());
                                                        }
                                                    }
                                                }
                                                if (lboolNode.has("must_not")) {
                                                    for (JsonNode mustnot : lboolNode.get("must_not")) {
                                                        if (!mustnot.findPath("user.location.type.keyword").isMissingNode()) {
                                                            //TODO: change continent to location type
                                                            lmbq.add(new TermQuery(new Term("continent", mustnot.get("term").get("user.location.type.keyword").get("value").asText())), BooleanClause.Occur.MUST_NOT);
                                                        } else {
                                                            logger.warn("unexpected geo field: " + mustnot.toString());
                                                        }
                                                    }
                                                }
                                                lbq.add(lmbq.build(), BooleanClause.Occur.SHOULD);
                                            } else {
                                                logger.warn("unexpected loc filter: " + loc.toString());
                                            }
                                        }
                                        bq.add(lbq.build(), BooleanClause.Occur.MUST);
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.current_experience.normed_titles").isMissingNode() ||
                                               !bNode.get("should").findPath("user.past_experience.titles").isMissingNode() ||
                                               !bNode.get("should").findPath("user.past_experience.normed_titles").isMissingNode()) {
                                        JsonNode titles = bNode.get("should");
                                        BooleanQuery.Builder tbq = new BooleanQuery.Builder();
                                        for(JsonNode title : titles) {
                                            if (!title.findPath("user.current_experience.titles").isMissingNode()) {
                                                tbq.add(new PhraseQuery("compound", split2Array(title.get("match_phrase").get("user.current_experience.titles").get("query").asText())), BooleanClause.Occur.SHOULD);
                                            } else if (!title.findPath("user.current_experience.normed_titles").isMissingNode()) {
                                                // TODO: check field name if necessary
                                                tbq.add(new PhraseQuery("compound", split2Array(title.get("match_phrase").get("user.current_experience.normed_titles").get("query").asText())), BooleanClause.Occur.SHOULD);
                                            } else if (!title.findPath("user.past_experience.titles").isMissingNode()) {
                                                tbq.add(new PhraseQuery("compound", split2Array(title.get("match_phrase").get("user.past_experience.titles").get("query").asText())), BooleanClause.Occur.SHOULD);
                                            } else if (!title.findPath("user.past_experience.normed_titles").isMissingNode()) {
                                                // TODO: check field name if necessary
                                                tbq.add(new PhraseQuery("compound", split2Array(title.get("match_phrase").get("user.past_experience.normed_titles").get("query").asText())), BooleanClause.Occur.SHOULD);
                                            } else {
                                                logger.warn("sth unexpected!!! " + title.toString());
                                            }
                                        }
                                        bq.add(tbq.build(), BooleanClause.Occur.MUST);
                                        c++;
                                    }
                                    if (!bNode.get("should").findPath("user.tags.experience_tag").isMissingNode()) {
                                        JsonNode hf = bNode.get("should");
                                        if (hf.size() > 1) {
                                            logger.info("multi field in json:" + hf.toString());
                                        }
                                        BooleanQuery.Builder termbq = new BooleanQuery.Builder();
                                        for(JsonNode h : hf) {
                                            JsonNode terms = h.get("terms");
                                            Iterator<String> tf = terms.fieldNames();
                                            while (tf.hasNext()) {
                                                String fieldName = tf.next();
                                                if (fieldName.equals("user.tags.experience_tag")) {
                                                    JsonNode yoes = terms.get("user.tags.experience_tag");
                                                    for (JsonNode yoe : yoes) {
                                                        termbq.add(new TermQuery(new Term("yoe", yoe.asText())), BooleanClause.Occur.SHOULD);
                                                    }
                                                } else if (fieldName.equals("boost")) {
                                                    continue;
                                                } else {
                                                    logger.warn("sth unexpected!!! " + terms.toString());
                                                }
                                            }
                                        }
                                        bq.add(termbq.build(), BooleanClause.Occur.MUST);
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
                                            bq.add(new TermQuery(new Term("uid", uid.asText())), BooleanClause.Occur.MUST_NOT);
                                        }
                                        c++;
                                    }
                                    if (!bNode.get("must_not").findPath("user.current_experience.companies").isMissingNode()) {
                                        JsonNode currentPositions = bNode.get("must_not");
                                        int k = currentPositions.size();
                                        if (!currentPositions.findPath("user.current_experience.companies").isMissingNode()) {
                                            bq.add(new PhraseQuery("compound", split2Array(currentPositions.findPath("user.current_experience.companies").get("query").asText())), BooleanClause.Occur.MUST_NOT);
                                            k --;
                                        }
                                        //TODO: correct field name - current titles
                                        if (!currentPositions.findPath("user.current_experience.titles").isMissingNode()) {
                                            bq.add(new PhraseQuery("compound", split2Array(currentPositions.findPath("user.current_experience.titles").get("query").asText())), BooleanClause.Occur.MUST_NOT);
                                            k--;
                                        }
                                        if (k != 0) {
                                            logger.info("unexpected must not phrase match: " + currentPositions.toString());
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
                if (!filterNode.isNull() && !filterNode.isEmpty() && filterNode.isArray()) {
                    for (JsonNode match : filterNode) {
                        if (!match.findPath("user.title_skill_search").isMissingNode()) {
                            bq.add(new PhraseQuery("compound", split2Array(match.get("match_phrase").get("user.title_skill_search").get("query").asText())), BooleanClause.Occur.SHOULD);
                        } else if (!match.findPath("user.tags.has_contact").isMissingNode() || !match.findPath("user.tags.has_personal_email").isMissingNode()) {
                            continue;
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
                                bq.add(IntPoint.newExactQuery("needSponsorship", 1), BooleanClause.Occur.MUST_NOT);
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

        return bq.build();
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
