package com.hiretual.search.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResponseUtils {

    static ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode putErrorJson(int code) {
        ObjectNode jo = mapper.createObjectNode();
        jo.putNull("data");
        jo.put("error_code", code);
        jo.put("error_message", GlobalPropertyUtils.get("global.error_message_" + code));
        return jo;
    }


    public static ObjectNode putSuccessJson(ArrayNode arrayNode) {
        ObjectNode jo = mapper.createObjectNode();
        jo.set("data", arrayNode);
        jo.put("error_code", 0);
        jo.put("error_message", "");
        return jo;
    }

    public static ObjectNode putSuccessJson(ObjectNode dataJO) {
        ObjectNode jo = mapper.createObjectNode();
        jo.putPOJO("data", dataJO);
        jo.put("error_code", 0);
        jo.put("error_message", "");
        return jo;
    }

}
