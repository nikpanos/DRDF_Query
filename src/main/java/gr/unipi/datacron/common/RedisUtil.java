package gr.unipi.datacron.common;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.*;

public class RedisUtil {
    public static TreeMap<Long, String> getSlotHostMap(String anyHostAndPortStr) {
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

    public static Hashtable<String, Pipeline> getPipeDictionary(Map<String, JedisPool> jedises) {
        Hashtable<String, Pipeline> result = new Hashtable<>(jedises.size());
        for (Map.Entry<String, JedisPool> jedisEntry: jedises.entrySet()) {
            Jedis jedis = jedisEntry.getValue().getResource();
            result.put(jedisEntry.getKey(), jedis.pipelined());
        }
        return result;
    }

    public static String getShardName(String key, TreeMap<Long, String> slotHostMap) {
        int slot = JedisClusterCRC16.getSlot(key);
        Map.Entry<Long, String> entry = slotHostMap.floorEntry(Long.valueOf(slot));
        return entry.getValue();
    }

    public static void syncAll(Collection<Pipeline> pipelines) {
        for (Pipeline pipe: pipelines) {
            pipe.sync();
        }
    }
}
