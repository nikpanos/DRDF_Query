package gr.unipi.datacron.store.utils;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.util.JedisClusterCRC16;

import java.util.*;

public class MyRedisCluster implements AutoCloseable {
    private JedisCluster jedisClu;
    private Map<String, JedisPool> nodeMap;
    private TreeMap<Long, String> slotHostMap;

    private Hashtable<String, Pipeline> pipes;

    public MyRedisCluster(Set<HostAndPort> hostAndPorts) {
        try {
            jedisClu = new JedisCluster(hostAndPorts);
            nodeMap = jedisClu.getClusterNodes();
            String anyHost = nodeMap.keySet().iterator().next();
            slotHostMap = getSlotHostMap(anyHost);
            pipes = getPipesDictionary(nodeMap);
        }catch (JedisClusterException e){
            e.printStackTrace();
        }
    }

    private Hashtable<String, Pipeline> getPipesDictionary(Map<String, JedisPool> jedises) {
        Hashtable<String, Pipeline> result = new Hashtable<>(jedises.size());
        for (Map.Entry<String, JedisPool> jedisEntry: jedises.entrySet()) {
            Jedis jedis = jedisEntry.getValue().getResource();
            result.put(jedisEntry.getKey(), jedis.pipelined());
        }
        return result;
    }

    private Jedis getJedis(String key) {
        return nodeMap.get(getShardName(key)).getResource();
    }

    private String getShardName(String key) {
        long slot = JedisClusterCRC16.getSlot(key);
        Map.Entry<Long, String> entry = slotHostMap.floorEntry(slot);
        return entry.getValue();
    }

    @SuppressWarnings("unchecked")
    private static TreeMap<Long, String> getSlotHostMap(String anyHostAndPortStr) {
        TreeMap<Long, String> tree = new TreeMap<>();
        String parts[] = anyHostAndPortStr.split(":");
        HostAndPort anyHostAndPort = new HostAndPort(parts[0], Integer.parseInt(parts[1]));
        try (Jedis jedis = new Jedis(anyHostAndPort.getHost(), anyHostAndPort.getPort())) {
            List<Object> list = jedis.clusterSlots();
            for (Object object : list) {
                List<Object> list1 = (List<Object>) object;
                List<Object> master = (List<Object>) list1.get(2);
                String hostAndPort = new String((byte[]) master.get(0)) + ":" + master.get(1);
                tree.put((Long) list1.get(0), hostAndPort);
                tree.put((Long) list1.get(1), hostAndPort);
            }
        }
        return tree;
    }

    public void wipeAllData() {
        for (JedisPool pool : nodeMap.values()) {
            try (Jedis jedis = pool.getResource()) {
                jedis.flushAll();
            }
            catch (Exception ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    public void syncPipes() {
        pipes.values().forEach(Pipeline::sync);
    }

    public void setNow(String key, String value) {
        try (Jedis jedis = getJedis(key)) {
            jedis.set(key, value);
        }
    }

    public String getNow(String key) {
        try (Jedis jedis = getJedis(key)) {
            return jedis.get(key);
        }
    }

    public String findInSetLowerNow(String key, long timestamp) {
        try (Jedis jedis = getJedis(key)) {
            return jedis.zrevrangeByScore(key, timestamp + "", "-inf", 0, 1).iterator().next();
        }
    }

    public String findInSetUpperNow(String key, long timestamp) {
        try (Jedis jedis = getJedis(key)) {
            return jedis.zrangeByScore(key, timestamp + "", "+inf", 0, 1).iterator().next();
        }
    }

    public Response<String> getLater(String key) {
        return pipes.get(getShardName(key)).get(key);
    }

    public Response<String> setLater(String key, String value) {
        return pipes.get(getShardName(key)).set(key, value);
    }

    @Override
    public void close() throws Exception {
        jedisClu.close();
        nodeMap.clear();
        slotHostMap.clear();
        pipes.clear();
    }
}
