package com.hiretual.search.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// import redis.clients.jedis.Pipeline;
// import redis.clients.jedis.Jedis;
// import redis.clients.jedis.JedisPool;


// @Component
public class JedisUtils{
    private static final Logger logger = LoggerFactory.getLogger(JedisUtils.class);
    // @Autowired
    // private JedisPool jedisPool;
    

   
   
   
    // public void set(String key, float[] array) {
    //     Jedis jedis = jedisPool.getResource();
    //     try{
    //         jedis.set(key.getBytes(), serialize(array));
    //     } finally {
    //       jedis.close();
    //     }
    // }

    // public float[] get(String key) {
    //     Jedis jedis = jedisPool.getResource();
    //     try{
    //         byte[] bytes=jedis.get(key.getBytes());
    //         float[]ret=unserialize(bytes);
    //         return ret;
    //     } catch(Exception e) {
    //         return null;
    //     } finally {
    //       jedis.close();
    //     }
    // }
    // public List<float[]>pipeline(List<String> keys){
    //     Jedis jedis = jedisPool.getResource();
    //     Pipeline pipeline = jedis.pipelined();
    //     List<float[]> ret = new ArrayList<>();
    //     for(String key :keys){
    //        pipeline.get(key.getBytes());
            
    //     }
    //     List<Object>list=pipeline.syncAndReturnAll();
    //     for(Object o:list){
    //         byte[] bytes=(byte[])o;
    //         ret.add(unserialize(bytes));
    //     }
    //     jedis.close();
    //     return ret;
    // }
    // public List<float[]> mget(List<String> keys) {
    //     Jedis jedis = jedisPool.getResource();
    //     byte[][] array = new byte[keys.size()][];
    //     for (int i = 0; i < keys.size(); i++){
            
    //         array[i] = keys.get(i).getBytes();
    //     }
    //     try{
            
    //         List<byte[]> list = jedis.mget(array);
            
    //         List<float[]> ret = new ArrayList<>();
    //         for (byte[] ba : list) {
    //             ret.add(unserialize(ba));
    //         }
    //         return ret;
    //     } catch(Exception e) {
    //         e.printStackTrace();
    //         return null;
    //     } finally {
    //        jedis.close();
    //     }
    // }

     static byte[] serialize(float[] array) {
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
    
     static float[] unserialize(byte[] bytes) {
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
 
  
}