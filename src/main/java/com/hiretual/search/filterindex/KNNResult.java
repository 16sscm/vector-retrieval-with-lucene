package com.hiretual.search.filterindex;

public class KNNResult {
    private final String uid;
    private final float score;
    private String rawJson;
    public KNNResult(final String uid, final float score,String rawJson) {
        this.uid = uid;
        this.score = score;
        this.rawJson=rawJson;
    }

    public String getRawJson(){
        return this.rawJson;
    }
    public String getUid() {
        return this.uid;
    }

    public float getScore() {
        return this.score;
    }
}
