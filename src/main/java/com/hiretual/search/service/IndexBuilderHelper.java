package com.hiretual.search.service;

import java.util.ArrayList;
import java.util.List;

import com.hiretual.search.model.DistributeInfo;
import com.hiretual.search.utils.GlobalPropertyUtils;

public class IndexBuilderHelper {
    DistributeInfo distributeInfo;
    private static String TOTAL_FILE_COUNT = GlobalPropertyUtils.get("total_file_count");
    private static int totalFileCount;
    static {
        totalFileCount=Integer.parseInt(TOTAL_FILE_COUNT);
    }
    public IndexBuilderHelper(DistributeInfo distributeInfo){
        this.distributeInfo=distributeInfo;
    } 
    private List<String> getHexStringList(){
        List<String>ret=new ArrayList<>();
        for(int i=0;i<totalFileCount;i++){
            if(i%distributeInfo.getTotalInstance()==distributeInfo.getSeqNum()){
                String strHex=Integer.toHexString(i);
                int len=strHex.length();
                StringBuilder builder=new StringBuilder();
                while(len<4){
                    builder.append('0');
                    len++;
                }
                ret.add(builder.toString()+strHex);
            }
        }
        return ret;
        
    }
    public  List<String> getRawJsonList(){
        List<String> hexStrs=getHexStringList();
        List<String> ret=new ArrayList<>();
        for(int i=0;i<hexStrs.size();i++){
            ret.add(hexStrs.get(i)+".json");
        }
        return ret;
    }
    public  List<String> getEmbeddingJsonList(){
        List<String> hexStrs=getHexStringList();
        List<String> ret=new ArrayList<>();
        for(int i=0;i<hexStrs.size();i++){
            ret.add(hexStrs.get(i)+"_embedding.json");
        }
        return ret;
    }
}
