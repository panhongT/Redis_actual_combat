package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */

@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Result sendcode(String phone, HttpSession session){
        //1.判断手机是否正确
        if (RegexUtils.isPhoneInvalid(phone)) {
            //验证失败，返回错误信息
            return Result.fail("手机格式输入错误！");
        }
        //2.生成验证码
        String code = RandomUtil.randomNumbers(6);
        //3.把验证码保存到redis，设置短信验证码有效期
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //4.发送验证码
        log.debug("发送短信验证码成功，验证码{}",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            //验证失败，返回错误信息
            return Result.fail("手机格式输入错误！");
        }
        //TODO 2.从redis获取验证码并校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (cacheCode == null || !cacheCode.equals(loginForm.getCode())){
            return Result.fail("验证码错误请重试！");
        }
        // 3.查询手机是否在数据库里
        User user = query().eq("phone", phone).one();
        //4.如果不在就把该用户注册进数据库
        if (user == null){
            user = creatUserByPhone(phone);

        }
        // 5.把用户放入redis
        //5.1随机生成token，作为登录令牌
        String token = UUID.fastUUID().toString(true);
        // 5.2将user对象转换为Hashmap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create().
                setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        // 5.3存储到redis
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token,userMap);
        //5.4设置token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL,TimeUnit.MINUTES);

        // 6返回token给客户端
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.获取本月截止今天为止所有的签到记录
        List<Long> results = stringRedisTemplate.opsForValue().bitField(
                key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (results == null || results.isEmpty()){
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num = results.get(0);
        if (num == null || num == 0){
            //没有任何签到结果
            return Result.ok(0);
        }
        //6.循环遍历
        int count = 0;
        while (true){
        //7.1让这个数字与1做与运算，得到数字的最后一个bit位,判断这个bit是否为0
            if ((num & 1) == 0){
                //7.2如果为0，说明未签到，循环结束
                break;
            }else {
                //7.3如果不为零，说明已签到，计数器+1
                count++;
            }
        //7.4把数字右移一位，把最后一个bit抛弃，继续统计下一个Bit
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User creatUserByPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        //为什么这里可以直接用save方法？
        //因为这个类继承了ServiceImpl<UserMapper, User>，这个mybatis-plus的类，所以可以直接用这个类里面的方法
        save(user);
        return user;
    }
}
