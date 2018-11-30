package com.zgl.hadoop.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 该类仅限制在redis内部操作数据
 *
 * @author zgl
 */
@Component
public class RedisUtils {

    private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    private static final long MAXLONG = 9007199254740992L;

    /**
     * 默认锁超时时间 30s
     */
    private static final long LOCK_EXPIRE = 30;

    /**
     * redis锁前缀
     */
    private static final String LOCK_PREFIX = "LOCK:";

    private static final String KEY_PREFIX = "OMS_BIGDATA_INFO:";
    private static final String ES_KEY_PREFIX = "OMS_BIGDATA_INFO:ES:INPUT_TABLE:";

    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    @Resource(name = "redisTemplate")
    private ValueOperations<String,Object> valueOperations;
    /**
     * 保存指定键值对
     *
     * @param redisKey
     * @param redisValue
     */
    public void save(String redisKey, String redisValue) {
        this.redisTemplate.opsForValue().set(redisKey, redisValue);
    }

    /**
     * 保存指定时间有效的键值对
     *
     * @param redisKey
     * @param redisValue
     * @param timeout
     * @param unit
     */
    public void save(String redisKey, String redisValue, long timeout, TimeUnit unit) {
        this.redisTemplate.setValueSerializer(new StringRedisSerializer());
        this.redisTemplate.opsForValue().set(redisKey, redisValue, timeout, unit);
    }


    public void saveObject(String redisKey,Object objectValue){
        valueOperations.set(redisKey,objectValue);
    }

    public Object getObject(String redisKey){
        return valueOperations.get(redisKey);
    }
    /**
     * 获取指定key的值
     *
     * @param redisKey
     * @return
     */
    public String get(String redisKey) {
        return this.redisTemplate.opsForValue().get(redisKey);
    }

    /**
     * 获取符合正则表达式的key的集合
     *
     * @param keyPattern
     * @return
     */
    public Set<String> keySets(String keyPattern) {
        return this.redisTemplate.keys(keyPattern);
    }

    /**
     * 删除指定的key
     *
     * @param hashKey
     * @param rangeKey
     */
    public void delete(String hashKey, long rangeKey) {
        String redisKey = this.getRedisKey(hashKey, rangeKey);
        this.redisTemplate.delete(redisKey);
        logger.info("remove redis key:[" + redisKey + "]");
    }

    /**
     * 删除指定的key
     *
     * @param redisKey
     */
    public void delete(String redisKey) {
        this.redisTemplate.delete(redisKey);
        logger.debug("remove redis key:[" + redisKey + "]");
    }

    /**
     * 删除指定的key并返回value
     *
     * @param redisKey
     */
    public String remove(String redisKey) {
        logger.debug("remove redis key:[" + redisKey + "]");
        String str = this.redisTemplate.opsForValue().get(redisKey);
        if (null != str) {
            this.redisTemplate.delete(redisKey);
        }
        return str;

    }

    /**
     * 删除指定的hash key( hashKey + ":")
     *
     * @param hashKey
     */
    public void deleteWithHashKey(String hashKey) {
        this.redisTemplate.delete(this.redisTemplate.keys(hashKey + ":*"));
        logger.info("remove redis key with hashKey:[" + hashKey + "]");
    }

    /**
     * 删除指定开始字符串的keys
     *
     * @param startKey
     */
    public void deleteWithStartKey(String startKey) {
        this.redisTemplate.delete(this.redisTemplate.keys(startKey + "*"));
        logger.info("remove redis key with hashKey:[" + startKey + "]");
    }

    /**
     * 指定key的过期时间
     *
     * @param redisKey
     * @param timeout
     * @param unit
     */
    public void expire(String redisKey, long timeout, TimeUnit unit) {
        this.redisTemplate.expire(redisKey, timeout, unit);
    }

    // SortedSet（有序集合）

    /**
     * 实现命令：ZADD key score member，将一个 member元素及其 score值加入到有序集 key当中。
     *
     * @param key
     * @param member
     * @param score
     */
    public void zAdd(String key, String member, double score) {
        this.redisTemplate.opsForZSet().add(key, member, score);
    }

    /**
     * zremove删除
     *
     * @param key
     * @param member
     */
    public void zRemove(String key, String... member) {
        this.redisTemplate.opsForZSet().remove(key, member);
    }


    /**
     * 返回指定元素的索引
     *
     * @param key
     * @param member
     */
    public Long zRank(String key, String member) {
        return this.redisTemplate.opsForZSet().rank(key, member);
    }


    /**
     * 实现命令：ZRANGE key start stop，返回有序集 key中，指定区间内的成员。
     *
     * @param key
     * @param sinceIdx
     * @param maxSize
     * @param scanIndexForward 从前面还是后面扫描
     * @return
     */
    public Set<String> zRangeByIdx(String key, long sinceIdx, long maxSize, boolean scanIndexForward) {
        if (scanIndexForward) {
            return this.redisTemplate.opsForZSet().range(key, sinceIdx, sinceIdx + maxSize - 1);
        } else {
            return this.redisTemplate.opsForZSet().range(key, (sinceIdx - maxSize) > 0 ? sinceIdx - maxSize + 1 : 0,
                    sinceIdx);
        }
    }

    /**
     * 实现命令：ZRANGE key start stop，返回有序集 key中，指定分数区间内的成员。
     *
     * @param key
     * @param sinceId
     * @param maxSize
     * @return
     */
    public Set<String> zRangeByScore(String key, long sinceId, int maxSize, boolean scanIndexForward) {
        int step = this.getStep(sinceId);
        if (scanIndexForward) {
            return this.redisTemplate.opsForZSet().rangeByScore(key, sinceId + step, Long.MAX_VALUE, 0, maxSize);
        } else {
            return this.redisTemplate.opsForZSet().reverseRangeByScore(key, 0,
                    sinceId > 1 ? (sinceId - step) : Long.MAX_VALUE, 0, maxSize);
        }
    }

    /**
     * 迭代zScan,结束时需要调用关闭方法
     *
     * @param key
     * @param scanOptions
     * @return
     */
    public Cursor<ZSetOperations.TypedTuple<String>> zScan(String key, ScanOptions scanOptions) {
        return redisTemplate.opsForZSet().scan(key, scanOptions);
    }

    /**
     * 关闭
     *
     * @param cursor
     */
    public void close(Cursor<ZSetOperations.TypedTuple<String>> cursor) {
        try {
            cursor.close();
        } catch (IOException e) {
            logger.error("关闭cursor失败", e);
        }
    }

    /**
     * 返回zset的长度
     *
     * @param key
     * @return
     */
    public long zSize(String key) {
        return this.redisTemplate.opsForZSet().size(key);
    }

    /**
     * @param hashKey
     * @param rangeKey
     * @return
     */
    public boolean hasKey(String hashKey, long rangeKey) {
        return this.redisTemplate.hasKey(this.getRedisKey(hashKey, rangeKey));
    }

    /**
     * @param redisKey
     * @return
     */
    public boolean hasKey(String redisKey) {
        return this.redisTemplate.hasKey(redisKey);
    }

    /**
     * @param redisKey key
     * @param delta    增量
     * @return
     */
    public long incr(String redisKey, long delta) {
        return this.redisTemplate.opsForValue().increment(redisKey, delta);
    }

    /**
     * @param hashKey
     * @param rangeKey
     * @return
     */
    private String getRedisKey(String hashKey, long rangeKey) {
        return hashKey + ":" + rangeKey;
    }

    private int getStep(long id) {
        if (Math.abs(id) <= MAXLONG) {
            return 1;
        } else {
            int step = 1;
            while (Math.abs(id) > (MAXLONG * step)) {
                step *= 10;
            }
            return step;
        }
    }

    /**
     * 在指定时间内尝试获取redis锁，成功返回true，失败返回false，并设置锁的过期时间
     *
     * @param key     锁标识
     * @param timeout 等待获取锁的最大时间（秒）
     * @param expire  锁过期失效时间（秒）
     * @return
     * @Description
     * @author congyue.lu
     */
    public boolean lock(String key, long timeout, long expire) {
        long begin = 0;
        do {
            if (this.tryLock(key, expire)) {
                return true;
            }
            begin += 3;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.error("获取锁出错", e);
                Thread.currentThread().interrupt();
                return false;
            }
        } while (begin <= timeout);
        return false;
    }

    /**
     * 尝试获取redis锁，成功返回true，失败返回false，并设置锁的过期时间
     *
     * @param key    锁标识
     * @param expire 锁过期失效时间（秒）
     * @return
     * @Description
     * @author congyue.lu
     */
    public boolean tryLock(String key, long expire) {
        logger.info("try get lock for:" + key);
        key = LOCK_PREFIX + key;
        RedisConnection redisConnection = this.redisTemplate.getConnectionFactory().getConnection();
        if (redisConnection.setNX(key.getBytes(), key.getBytes())) {
            this.redisTemplate.expire(key, expire, TimeUnit.SECONDS);
            redisConnection.close();
            return true;
        }
        redisConnection.close();
        return false;
    }

    /**
     * 在指定时间内尝试获取redis锁，成功返回true，失败返回false
     *
     * @param key     锁标识
     * @param timeout 等待获取锁的最大时间（秒）
     * @return
     * @Description
     * @author congyue.lu
     */
    public boolean lock(String key, long timeout) {
        return this.lock(key, timeout, LOCK_EXPIRE);
    }

    /**
     * 尝试获取redis锁，成功返回true，失败返回false
     *
     * @param key 锁标识
     * @return
     * @Description
     * @author congyue.lu
     */
    public boolean tryLock(String key) {
        return this.tryLock(key, LOCK_EXPIRE);
    }

    /**
     * redis锁解锁
     *
     * @param key 锁标识
     * @Description
     * @author congyue.lu
     */
    public void unLock(String key) {
        key = LOCK_PREFIX + key;
        this.redisTemplate.delete(key);
    }


    /**
     * redis发布消息
     *
     * @param channel
     * @param message
     */
    public void publish(String channel, String message) {
        redisTemplate.convertAndSend(channel, message);
    }

    /**
     * 获取指定区间位置的list元素
     *
     * @param key
     * @return
     */
    public List<String> range(String key, long start, long end) {
        return redisTemplate.opsForList().range(key, start, end);
    }

    /**
     * 获取指定list的长度
     *
     * @param key
     * @return
     */
    public long listSize(String key) {
        Long size = redisTemplate.opsForList().size(key);
        if (size == null) {
            return 0L;
        }
        return size;
    }

    /**
     * 向指定的列表左边插入数据
     *
     * @param key
     * @param value
     * @return
     */
    public void leftPush(String key, String value) {
        redisTemplate.opsForList().leftPush(key, value);
    }

    /**
     * 向指定的列表左边插入数据
     *
     * @param key
     * @param value
     * @return
     */
    public void rightPush(String key, String value) {
        redisTemplate.opsForList().rightPush(key, value);
    }

    /**
     * 弹出指定列表右边的数据
     *
     * @param key
     * @return
     */
    public String rightPop(String key) {
        return redisTemplate.opsForList().rightPop(key);
    }

    /**
     * 弹出指定列表右边的数据(如果没有数据,在指定的时间内等待)
     *
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    public String rightPop(String key, long timeout, TimeUnit unit) {
        return redisTemplate.opsForList().rightPop(key, timeout, unit);
    }


    /**
     * 弹出指定列表右边,并向指定列表的左边插入
     *
     * @param sourceKey
     * @param destinationKey
     * @return
     */
    public String rightPopAndLeftPush(String sourceKey, String destinationKey) {
        return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey);
    }

    /**
     * 弹出指定列表右边,并向指定列表的左边插入(弹出列表如果没有元素,等待指定的时间)
     *
     * @param sourceKey
     * @param destinationKey
     * @param timeout
     * @param unit
     * @return
     */
    public String rightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit) {
        return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
    }


    /**
     * 存储hash形式的键值对
     *
     * @param key
     * @param hash
     * @param value
     */
    public void saveHashKey(String key, String hash, String value) {
        redisTemplate.opsForHash().put(key, hash, value);
    }

    /**
     * 存储hash形式的键值对(如果有null直接放弃存储)
     *
     * @param key
     * @param hash
     * @param value
     */
    public void saveHashKey4Null(String key, String hash, String value) {
        if (null == key || null == hash || null == value) {
            return;
        }
        redisTemplate.opsForHash().put(key, hash, value);
    }

    /**
     * 删除hash
     *
     * @param key
     * @param hash
     */
    public void delHashKey(String key, String... hash) {
        redisTemplate.opsForHash().delete(key, hash);
    }

    /**
     * 获取hash形式的value
     *
     * @param key
     * @param hash
     */
    public String getHashKey(String key, String hash) {
        Object o = redisTemplate.opsForHash().get(key, hash);
        if (null != o) {
            return o.toString();
        }
        return null;
    }

    public String getRedisKey(String key) {
        return  KEY_PREFIX + key;
    }

    public String getESRedisKey(String key) {
        return  ES_KEY_PREFIX + key;
    }
}
