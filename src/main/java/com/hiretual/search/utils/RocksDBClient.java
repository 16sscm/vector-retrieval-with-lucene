package com.hiretual.search.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
@Component
public class RocksDBClient {
    static {
        RocksDB.loadLibrary();
    }
    private static Logger logger=LoggerFactory.getLogger(RocksDBClient.class);
    private static RocksDB rocksDB;
    private static String path = GlobalPropertyUtils.get("rocksDB");
    static Options options = new Options();
    static {
        options.setCreateIfMissing(true);
        try {
            rocksDB = RocksDB.open(options,path);
        } catch (RocksDBException e) {
            logger.error("fail to init rocketsDB ",e);
           
        }
    }
    public void set(String key, float[] array) {
            try {
                rocksDB.put(key.getBytes(), JedisUtils.serialize(array));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
       
    }
    public void batchSet(Map<String, float[]> map) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (Map.Entry<String, float[]> entry : map.entrySet()) {
                writeBatch.put(entry.getKey().getBytes(), JedisUtils.serialize(entry.getValue()));
            }
            rocksDB.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            logger.error("fail to batch put", e);
        }

    }

    public float[] get(String key) {
        try {
            return JedisUtils.unserialize(rocksDB.get(key.getBytes()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
   
}
    public List<float[]>multiGetAsList(List<String> keys){
        List<byte[]>keyList=new ArrayList<>();
        for (int i = 0; i < keys.size(); i++){
            
            keyList.add(keys.get(i).getBytes()) ;
        }
        try {
            List<byte[]>valueList=rocksDB.multiGetAsList(keyList);
            List<float[]> ret = new ArrayList<>();
            for (byte[] ba : valueList) {
                ret.add(JedisUtils.unserialize(ba));
            }
            return ret;
        } catch (RocksDBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
        
    }
    public static void shutdown(){
        rocksDB.close();
    }
    public static void main(String[] args) throws RocksDBException {
        RocksDBClient rockeDBClient=new RocksDBClient();
        // float[] a_float=new float[]{1f,2f};
        // float[]b_float=new float[]{3f,4f};
        // rockeDBClient.set("a",a_float);
        // rockeDBClient.set("b",b_float);
       
        // System.out.println("-----");
        // float[] vals = rockeDBClient.get("b");
        // for(float val:vals){
        //     System.out.println(val);
        // }
        List<String> keys=new ArrayList<>();
        keys.add("00045e73-65a2-440d-aa68-28be2f7ca116");
        keys.add("b");
        List<float[]>vecs=rockeDBClient.multiGetAsList(keys);
        for(float[] vec:vecs){
            for(float val:vec){
                System.out.println(val);
            }
        }
       
    }
}
