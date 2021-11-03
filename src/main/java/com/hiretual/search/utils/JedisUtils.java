package com.hiretual.search.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

@Component
public class JedisUtils{
    private static final Logger logger = LoggerFactory.getLogger(JedisUtils.class);
    @Autowired
    private JedisPool jedisPool;

    public void set(String key, float[] array) {
        Jedis jedis = jedisPool.getResource();
        try{
            jedis.set(key.getBytes(), serialize(array));
        } finally {
            jedisPool.returnResource(jedis);
            // jedis.close();
        }
    }

    public float[] get(String key) {
        Jedis jedis = jedisPool.getResource();
        try{
            return unserialize(jedis.get(key.getBytes()));
        } catch(Exception e) {
            return null;
        } finally {
            // jedisPool.returnResource(jedis);
            jedis.close();
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
}