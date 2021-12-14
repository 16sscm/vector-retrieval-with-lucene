package com.hiretual.search.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class RequestParser {
    private static final Logger logger = LoggerFactory.getLogger(RequestParser.class);

    public static JsonNode getPostParameter(HttpServletRequest request) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode returnNode = mapper.createObjectNode();
        try {
            InputStream is = request.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            StringBuffer content = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
            String parameterStr = content.toString().trim();
//            logger.info(parameterStr);
            returnNode = mapper.readTree(parameterStr);
        } catch (Exception e) {
            logger.error("fail to parse parameter string to json object", e);
        }

        return returnNode;
    }

    public static JsonNode getPostParameter(String parameterStr) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode returnNode = mapper.createObjectNode();
        try {
            returnNode = mapper.readTree(parameterStr);
        } catch (Exception e) {
            logger.error("fail to parse parameter string to json object|" + parameterStr, e);
        }

        return returnNode;
    }
    public static String getJsonString( Object object){
        ObjectMapper mapper = new ObjectMapper(); 
        String str="";
        try {
            str = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return str;
    }
    public static List<Double> transformJson2Array(String jsonString){
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<Double> list = mapper.readValue(jsonString,new TypeReference<List<Double>>() { });
            return list;
        } catch (JsonProcessingException e) {
            logger.error("fail to parse json string to KNNResult list|" + jsonString, e);
            return null;
        }

    }
}
