package com.hiretual.search.filterindex;

public class FakeDocument {
    float[]vector;
    String id;
    String title;
    long price;

    public float[] getVector() {
        return vector;
    }


    public void setVector(float[] vector) {
        this.vector = vector;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}

