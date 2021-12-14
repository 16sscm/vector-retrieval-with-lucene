package com.hiretual.search.model;

public class DistributeInfo {
    int totalInstance;
    int seqNum;
    public DistributeInfo(){
        
    }
    public DistributeInfo(int totalInstance,int seqNum){
        this.seqNum=seqNum;
        this.totalInstance=totalInstance;
    }
    public void setTotalInstance(int totalInstance){
        this.totalInstance=totalInstance;

    }
    public void setSeqNum(int seqNum){
        this.seqNum=seqNum;
    }
    public int getSeqNum(){
        return this.seqNum;
    }

    public int getTotalInstance(){
        return this.totalInstance;
    }
}
