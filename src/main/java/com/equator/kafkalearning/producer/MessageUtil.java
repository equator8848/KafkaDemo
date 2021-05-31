package com.equator.kafkalearning.producer;

import java.util.Date;

/**
 * @Author: Equator
 * @Date: 2021/5/31 22:15
 **/

public class MessageUtil {
    public static String getMsg() {
        return String.valueOf(new Date().getTime());
    }

    public static void main(String[] args) {
        System.out.println(getMsg());
    }
}
