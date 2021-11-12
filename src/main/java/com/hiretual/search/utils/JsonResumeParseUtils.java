package com.hiretual.search.utils;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonResumeParseUtils {

    public static boolean isJsonNodeNull(JsonNode jsonNode, String fieldName) {
        return jsonNode.isNull() || !jsonNode.has(fieldName) || jsonNode.get(fieldName).isNull();
    }

    /**
     * 获取boolean类型的JsonNode的某字段，默认值为false
     * @param jsonNode
     * @param fieldName
     * @return
     */
    public static boolean getBoolFieldFromJsonNode(JsonNode jsonNode, String fieldName) {
        if (isJsonNodeNull(jsonNode, fieldName)) {
            return false;
        }
        return jsonNode.get(fieldName).asBoolean();
    }

    /**
     * 获取int类型的JsonNode的某字段，默认值为0
     * @param jsonNode
     * @param fieldName
     * @return
     */
    public static int getIntFieldFromJsonNode(JsonNode jsonNode, String fieldName) {
        if (isJsonNodeNull(jsonNode, fieldName)) {
            return 0;
        }
        return jsonNode.get(fieldName).asInt();
    }

    /**
     * 获取String类型的JsonNode的某字段，默认值为null
     * @param jsonNode
     * @param fieldName
     * @return
     */
    public static String getStringFieldFromJsonNode(JsonNode jsonNode, String fieldName) {
        if (isJsonNodeNull(jsonNode, fieldName)) {
            return null;
        }
        return jsonNode.get(fieldName).asText();
    }

    /**
     * 获取String类型的JsonNode的某字段，需自定义默认值
     * @param jsonNode
     * @param fieldName
     * @param defaultValue 自定义默认值
     * @return
     */
    public static String getStringFieldFromJsonNode(JsonNode jsonNode, String fieldName, String defaultValue) {
        if (isJsonNodeNull(jsonNode, fieldName)) {
            return defaultValue;
        }
        return jsonNode.get(fieldName).asText();
    }
}
