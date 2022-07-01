package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.NonNull;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    /*
    因为这个类继承的是mybatis-plus的类，所以这个类是由spring管理的，可以直接把StringRedisTemplate注入给它
     */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
//        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
        if (shop == null){
            return  Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
    //解决缓存穿透的方法queryWithPassThrougu()
    public Shop queryWithPassThrougu(Long id){
        //1.从redis查询商铺缓存
        String shopJason = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //2.判断缓存是否命中
        if (StrUtil.isNotBlank(shopJason)) {
            //3.如果命中直接返回商铺信息
            Shop shop = JSONUtil.toBean(shopJason, Shop.class);
            return shop;
        }
        //判断是否为空值，因为前面已经判断了isNotBlank存在的两种情况，一个是null，一个是""，所以这里只需要判断不等于null就是""
        if (shopJason != null){
            //返回一个错误信息
            return null;
        }
        //4.没有命中就根据id去数据库查
        //为什么这里可以直接用getById方法？
        //因为这个类继承了ServiceImpl<UserMapper, User>，这个mybatis-plus的类，所以可以直接用这个类里面的方法
        Shop shop = getById(id);
        //5.判断商铺是否存在
        if (shop == null){
            //6.将空值写入redis缓存
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //7.如果存在就把数据写入redis
        String jsonStr = JSONUtil.toJsonStr(shop);
        //加入超时时间，做到超时剔除
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,jsonStr,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //8.返回商品信息
        return shop;
    }
    //互斥锁解决缓存击穿的方法queryWithMutex()
    public Shop queryWithMutex(Long id){
        //1.从redis查询商铺缓存
        String shopJason = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //2.判断缓存是否命中
        if (StrUtil.isNotBlank(shopJason)) {
            //3.如果命中直接返回商铺信息
            Shop shop = JSONUtil.toBean(shopJason, Shop.class);
            return shop;
        }
        //判断是否为空值，因为前面已经判断了isNotBlank存在的两种情况，一个是null，一个是""，所以这里只需要判断不等于null就是""
        if (shopJason != null){
            //返回一个错误信息
            return null;
        }
        //4.获取锁
        String lockKey = LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.1如果获取锁失败，休眠一段时间并重试
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.2如果获取锁成功，根据id查数据库，并进行缓存重建
            shop = getById(id);
            //5.判断商铺是否存在
            if (shop == null){
                //6.将空值写入redis缓存
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //7.如果存在就把数据写入redis
            String jsonStr = JSONUtil.toJsonStr(shop);
            //加入超时时间，做到超时剔除
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,jsonStr,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw  new RuntimeException(e);
        }finally {
            //8.释放互斥锁
            unlock(lockKey);
        }
        //9.返回商品信息
        return shop;
    }
    private static final ExecutorService CACHE_REBUILE_EXECUTOR = Executors.newFixedThreadPool(10);
    //逻辑过期解决缓存过期的方法queryWithLogicalExpire()
    public Shop queryWithLogicalExpire(Long id){
        //1.从redis查询商铺缓存
        String shopJason = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //2.判断缓存是否命中
        if (StrUtil.isBlank(shopJason)) {
            //3.如果不存在直接返回null
            return null;
        }
        //4.如果命中，需要将shopJason反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJason, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，直接返回店铺信息
            return shop;
        }
        //5.2已过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(lockKey);
        //6.2判断是否获取锁成功
        if (tryLock) {
            //6.3成功，开启线程，实现缓冲重建
            CACHE_REBUILE_EXECUTOR.submit(() ->{
                try {
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw  new RuntimeException();
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //6.4失败，直接返回（过期的）
        return shop;
    }

    public void saveShop2Redis(Long id,Long expireSeconds){
        //1.查询数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis,没有设置过期时间，可以认为这个缓存是永久存在的
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(false);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("更新失败:店铺id不能为空");
        }
        //1.操作数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();

    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null){
            //不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询reis，按照距离排序、分页。结果：shopid、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        //4.解析id
        if (results == null){
            return  Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            //没有下一条了
            return  Result.ok(Collections.emptyList());
        }
        //4.1截取from - end 部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String , Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result ->{
            //4.2获取店铺ID
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3获取店铺距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")")
                .list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }
}
