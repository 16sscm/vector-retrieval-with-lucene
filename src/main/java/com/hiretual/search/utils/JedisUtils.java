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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Jedis;


public class JedisUtils{
    private static final Logger logger = LoggerFactory.getLogger(JedisUtils.class);
    // @Autowired
    // private JedisPool jedisPool;

    Jedis jedis ;
    public JedisUtils(){
        jedis = new Jedis("10.100.10.19",9222,5000);
    }
   
    public void set(String key, float[] array) {
       
        try{
            jedis.set(key.getBytes(), serialize(array));
        } finally {
          
        }
    }

    public float[] get(String key) {
     
        try{
            return unserialize(jedis.get(key.getBytes()));
        } catch(Exception e) {
            return null;
        } finally {
          
        }
    }
    public List<float[]>pipeline(List<String> keys){
        // Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        List<float[]> ret = new ArrayList<>();
        for(String key :keys){
           pipeline.get(key.getBytes());
            
        }
        List<Object>list=pipeline.syncAndReturnAll();
        for(Object o:list){
            byte[] bytes=(byte[])o;
            ret.add(unserialize(bytes));
        }

        return ret;
    }
    public List<float[]> mget(List<String> keys) {
        
      
        byte[][] array = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++){
            
            array[i] = keys.get(i).getBytes();
        }
        try{
            
            List<byte[]> list = jedis.mget(array);
            
            List<float[]> ret = new ArrayList<>();
            for (byte[] ba : list) {
                ret.add(unserialize(ba));
            }
            return ret;
        } catch(Exception e) {
            return null;
        } finally {
           
        }
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
    public String debsize(){
        return jedis.dbSize().toString();
    }
    public void close(){
        jedis.close();
    }
}