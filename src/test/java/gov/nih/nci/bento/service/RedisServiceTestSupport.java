package gov.nih.nci.bento.service;

import gov.nih.nci.bento.model.ConfigurationDAO;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Shared setup for {@link RedisService} unit tests (mocked Jedis, no live Redis).
 */
final class RedisServiceTestSupport {

    private RedisServiceTestSupport() {
    }

    static RedisService newService(ConfigurationDAO config) throws Exception {
        RedisService service = new RedisService();
        setField(service, "config", config);
        return service;
    }

    static void setPool(RedisService service, JedisPool pool) throws Exception {
        setField(service, "pool", pool);
        setField(service, "useCluster", false);
        setField(service, "isInitialized", true);
    }

    static void setCluster(RedisService service, JedisCluster cluster) throws Exception {
        setField(service, "cluster", cluster);
        setField(service, "useCluster", true);
        setField(service, "isInitialized", true);
    }

    static void setClusterOnly(RedisService service, JedisCluster cluster) throws Exception {
        setField(service, "cluster", cluster);
    }

    static void setTtl(RedisService service, int ttl) throws Exception {
        setField(service, "ttl", ttl);
    }

    static Object invokePrivate(RedisService service, String methodName, Class<?>[] paramTypes, Object... args)
            throws Exception {
        Method method = RedisService.class.getDeclaredMethod(methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(service, args);
    }

    static void close(RedisService service) throws Exception {
        invokePrivate(service, "close", new Class[] {});
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
