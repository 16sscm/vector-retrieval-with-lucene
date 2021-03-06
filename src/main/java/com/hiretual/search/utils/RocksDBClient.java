package com.hiretual.search.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final String INDEX_SAVE_DIR=GlobalPropertyUtils.get("index_save_dir");
    private static String path = INDEX_SAVE_DIR+GlobalPropertyUtils.get("rocksDB");
    private static String mode = GlobalPropertyUtils.get("mode");
    static Options options = new Options();
    static {
        options.setCreateIfMissing(true);
        options.setMaxBackgroundFlushes(4);
        options.setMaxBackgroundCompactions(4);
        options.setMaxWriteBufferNumber(8);
        options.setMinWriteBufferNumberToMerge(4);

        try {
            if(mode.equals("search")){
                logger.info("search mode,open no rocksDB");
            }
            else{
                rocksDB = RocksDB.open(options,path);
                logger.info("Open rocksDB in write mode,write to rocksDB,path:"+path);
            }
            
        } catch (RocksDBException e) {
            logger.error("fail to init rocketsDB ",e);
           
        }
    }
    public void set(String key, float[] array) {
            try {
                rocksDB.put(key.getBytes(), serialize(array));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
       
    }
    public void batchSet(Map<String, float[]> map) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (Map.Entry<String, float[]> entry : map.entrySet()) {
                writeBatch.put(entry.getKey().getBytes(), serialize(entry.getValue()));
            }
            rocksDB.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            logger.error("fail to batch put", e);
        }

    }

    public float[] get(String key) {
        try {
            return unserialize(rocksDB.get(key.getBytes()));
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
                ret.add(unserialize(ba));
            }
            return ret;
        } catch (RocksDBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
        
    }

    private static byte[] serialize(float[] array) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(array);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            logger.error("fail to serialize the vector: " + Arrays.toString(array));
        }
        return null;
    }

    private static float[] unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (float[]) ois.readObject();
        } catch (Exception e) {
            logger.error("fail to unserialize the vector: " + bytes);
        }
        return null;
    }

    public static void shutdown(){
        
        rocksDB.close();
    }
   
}
