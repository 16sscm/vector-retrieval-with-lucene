package com.hiretual.search.model;



public class FilterQuery {
    String[] black_uids;
    public FilterQuery(String[] black_uids){
        this.black_uids=black_uids;
    }
    public String[] getBlack_uids() {
        return black_uids;
    }
}
