package gov.nih.nci.bento.service;

import gov.nih.nci.bento.model.ConfigurationDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisService} with mocked Jedis pool/cluster clients.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisServiceTest {

    private ConfigurationDAO disabledRedisConfig() {
        ConfigurationDAO config = mock(ConfigurationDAO.class);
        when(config.isRedisEnabled()).thenReturn(false);
        return config;
    }

    private ConfigurationDAO enabledPoolConfig(int ttl) {
        ConfigurationDAO config = mock(ConfigurationDAO.class);
        when(config.isRedisEnabled()).thenReturn(true);
        when(config.getRedisHost()).thenReturn("localhost");
        when(config.getRedisPort()).thenReturn(6379);
        when(config.isRedisUseCluster()).thenReturn(false);
        when(config.getRedisTTL()).thenReturn(ttl);
        return config;
    }

    private ConfigurationDAO enabledClusterConfig(int ttl) {
        ConfigurationDAO config = mock(ConfigurationDAO.class);
        when(config.isRedisEnabled()).thenReturn(true);
        when(config.getRedisHost()).thenReturn("localhost");
        when(config.getRedisPort()).thenReturn(6379);
        when(config.isRedisUseCluster()).thenReturn(true);
        when(config.getRedisTTL()).thenReturn(ttl);
        return config;
    }

    @Test
    void init_redisDisabled_leavesServiceUninitialized() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(disabledRedisConfig());

        service.init();

        assertFalse(service.isInitialized());
    }

    @Test
    void init_blankHost_leavesServiceUninitialized() throws Exception {
        ConfigurationDAO config = mock(ConfigurationDAO.class);
        when(config.isRedisEnabled()).thenReturn(true);
        when(config.getRedisHost()).thenReturn("   ");
        RedisService service = RedisServiceTestSupport.newService(config);

        service.init();

        assertFalse(service.isInitialized());
    }

    @Test
    void init_poolMode_createsJedisPool() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(3600));

        try (MockedConstruction<JedisPool> ignored = mockConstruction(JedisPool.class)) {
            service.init();
        }

        assertTrue(service.isInitialized());
    }

    @Test
    void init_clusterMode_createsJedisCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(3600));

        try (MockedConstruction<JedisCluster> ignored = mockConstruction(JedisCluster.class)) {
            service.init();
        }

        assertTrue(service.isInitialized());
    }

    @Test
    void cacheValue_stringWithTtl_usesSetexOnPool() throws Exception {
        ConfigurationDAO config = enabledPoolConfig(3600);
        RedisService service = RedisServiceTestSupport.newService(config);
        RedisServiceTestSupport.setTtl(service, 3600);
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        RedisServiceTestSupport.setPool(service, pool);

        service.cacheValue("my key", "payload");

        verify(jedis).setex("my_key", 3600, "payload");
        verify(jedis).close();
    }

    @Test
    void cacheValue_stringWithoutTtl_usesSetOnPool() throws Exception {
        ConfigurationDAO config = enabledPoolConfig(0);
        RedisService service = RedisServiceTestSupport.newService(config);
        RedisServiceTestSupport.setTtl(service, 0);
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        RedisServiceTestSupport.setPool(service, pool);

        service.cacheValue("cache-key", "value");

        verify(jedis).set("cache-key", "value");
    }

    @Test
    void cacheValue_set_usesSaddWithHashTagOnPool() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        RedisServiceTestSupport.setPool(service, pool);

        service.cacheValue("group:key", new String[] {"a", "b"}, true);

        verify(jedis).sadd("{set}.group:key", "a", "b");
    }

    @Test
    void cacheValue_jedisException_doesNotPropagate() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenThrow(new JedisException("write failed"));
        RedisServiceTestSupport.setPool(service, pool);

        service.cacheValue("key", "value");
    }

    @Test
    void cacheValue_uninitializedRedis_doesNotThrow() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));

        service.cacheValue("key", "value");
    }

    @Test
    void getCachedValue_returnsSanitizedKeyLookup() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.get("foo_bar")).thenReturn("cached");
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals("cached", service.getCachedValue("foo\" bar"));
    }

    @Test
    void getCachedValue_uninitializedRedis_returnsNull() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));

        assertNull(service.getCachedValue("missing"));
    }

    @Test
    void getCachedSet_returnsTaggedSetMembers() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.smembers("{set}.participants")).thenReturn(Set.of("p1", "p2"));
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(Set.of("p1", "p2"), service.getCachedSet("participants"));
    }

    @Test
    void getUnion_returnsTaggedUnionFromPool() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.sunion("{set}.a", "{set}.b")).thenReturn(Set.of("x"));
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(Set.of("x"), service.getUnion(new String[] {"a", "b"}));
    }

    @Test
    void getIntersection_returnsTaggedIntersectionFromPool() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.sinter("{set}.a", "{set}.b")).thenReturn(Set.of("shared"));
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(Set.of("shared"), service.getIntersection(new String[] {"a", "b"}));
    }

    @Test
    void getUnion_jedisException_returnsNull() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenThrow(new JedisException("read failed"));
        RedisServiceTestSupport.setPool(service, pool);

        assertNull(service.getUnion(new String[] {"a", "b"}));
    }

    @Test
    void formatKey_removesQuotesAndSpaces() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(disabledRedisConfig());

        assertEquals("foo_bar", RedisServiceTestSupport.invokePrivate(service, "formatKey", new Class[] {String.class}, "foo\" bar"));
        assertArrayEquals(
                new String[] {"a_b", "c_d"},
                (String[]) RedisServiceTestSupport.invokePrivate(
                        service, "formatKeys", new Class[] {String[].class}, (Object) new String[] {"a b", "c d"}));
        assertEquals("{set}.key", RedisServiceTestSupport.invokePrivate(service, "addSetHashTag", new Class[] {String.class}, "key"));
    }

    @Test
    void unionStore_returnsStoredCount() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(mock(ConfigurationDAO.class));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.sunionstore(eq("{set}.result"), any(String[].class))).thenReturn(3L);
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(3L, service.unionStore("result", new String[] {"a", "b"}));
    }

    @Test
    void unionStore_zeroResult_skipsInfoLog() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(mock(ConfigurationDAO.class));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.sunionstore(eq("{set}.empty"), any(String[].class))).thenReturn(0L);
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(0L, service.unionStore("empty", new String[] {"a"}));
    }

    @Test
    void interStore_returnsStoredCount() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.sinterstore(eq("{set}.result"), any(String[].class))).thenReturn(2L);
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(2L, service.interStore("result", new String[] {"a", "b"}));
    }

    @Test
    void unionStore_jedisException_returnsZero() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenThrow(new JedisException("store failed"));
        RedisServiceTestSupport.setPool(service, pool);

        assertEquals(0L, service.unionStore("result", new String[] {"a"}));
    }

    @Test
    void cacheGroup_tracksGroupPrefix() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        RedisServiceTestSupport.setPool(service, pool);

        service.cacheGroup("cohort:all", new String[] {"p1"});

        assertArrayEquals(new String[] {"cohort"}, service.getGroups());
    }

    @Test
    void cacheValue_clusterModeWithTtl_usesClusterSetex() throws Exception {
        ConfigurationDAO config = enabledClusterConfig(120);
        RedisService service = RedisServiceTestSupport.newService(config);
        RedisServiceTestSupport.setTtl(service, 120);
        JedisCluster cluster = mock(JedisCluster.class);
        RedisServiceTestSupport.setCluster(service, cluster);

        service.cacheValue("cluster-key", "payload");

        verify(cluster).setex("cluster-key", 120, "payload");
    }

    @Test
    void cacheValue_clusterModeWithoutTtl_usesClusterSet() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        RedisServiceTestSupport.setTtl(service, 0);
        JedisCluster cluster = mock(JedisCluster.class);
        RedisServiceTestSupport.setCluster(service, cluster);

        service.cacheValue("cluster-key", "payload");

        verify(cluster).set("cluster-key", "payload");
    }

    @Test
    void cacheValue_clusterSet_usesClusterSadd() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        RedisServiceTestSupport.setCluster(service, cluster);

        service.cacheValue("group:key", new String[] {"v1"}, true);

        verify(cluster).sadd("{set}.group:key", "v1");
    }

    @Test
    void getCachedValue_clusterMode_readsFromCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.get("key")).thenReturn("cluster-value");
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals("cluster-value", service.getCachedValue("key"));
    }

    @Test
    void getCachedSet_clusterMode_readsTaggedSetFromCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.smembers("{set}.ids")).thenReturn(Set.of("1"));
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals(Set.of("1"), service.getCachedSet("ids"));
    }

    @Test
    void getUnion_clusterMode_readsTaggedUnionFromCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.sunion(any(String[].class))).thenReturn(Set.of("u"));
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals(Set.of("u"), service.getUnion(new String[] {"a", "b"}));
    }

    @Test
    void getIntersection_clusterMode_readsTaggedIntersectionFromCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.sinter(any(String[].class))).thenReturn(Set.of("i"));
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals(Set.of("i"), service.getIntersection(new String[] {"a", "b"}));
    }

    @Test
    void unionStore_clusterMode_usesClusterSunionstore() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.sunionstore(eq("{set}.merged"), any(String[].class))).thenReturn(4L);
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals(4L, service.unionStore("merged", new String[] {"a", "b"}));
    }

    @Test
    void interStore_clusterMode_usesClusterSinterstore() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledClusterConfig(0));
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.sinterstore(eq("{set}.merged"), any(String[].class))).thenReturn(1L);
        RedisServiceTestSupport.setCluster(service, cluster);

        assertEquals(1L, service.interStore("merged", new String[] {"a", "b"}));
    }

    @Test
    void close_closesPoolAndCluster() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(enabledPoolConfig(0));
        JedisPool pool = mock(JedisPool.class);
        JedisCluster cluster = mock(JedisCluster.class);
        RedisServiceTestSupport.setPool(service, pool);
        RedisServiceTestSupport.setClusterOnly(service, cluster);

        RedisServiceTestSupport.close(service);

        verify(pool).close();
        verify(cluster).close();
    }

    @Test
    void groupListAndParameterMappings_accessorsWork() throws Exception {
        RedisService service = RedisServiceTestSupport.newService(disabledRedisConfig());
        HashMap<String, String> mappings = new HashMap<>();
        mappings.put("race", "race_str");

        service.setGroupListsInitialized(true);
        service.setParameterMappings(mappings);

        assertTrue(service.isGroupListsInitialized());
        assertEquals("race_str", service.getParameterMappings().get("race"));
    }
}
