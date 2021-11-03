package com.hiretual.search.filterindex;

public class KNNResult {
    private final String uid;
    private final float score;

    public KNNResult(final String uid, final float score) {
        this.uid = uid;
        this.score = score;
    }

    public String getUid() {
        return this.uid;
    }

    public float getScore() {
        return this.score;
    }
}
