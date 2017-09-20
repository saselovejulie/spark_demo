package com.leo.test;

import com.google.gson.Gson;
import com.leo.entity.InspurUserEntity;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by xucongjie on 2017/9/14.
 */
public class LocalTest {

    public static void main(String[] args) throws IOException {
        Gson gson = new Gson();
        FileWriter fw = new FileWriter("E:\\TEMP\\hbase_data.txt");
        int maxCount = 1000000;
        for (int i = 1; i < maxCount; i++) {
            InspurUserEntity inspurUserEntity = new InspurUserEntity();
            StringBuilder sb = new StringBuilder();
            inspurUserEntity.setId(String.format("%08d", i));
            inspurUserEntity.setAge("27");
            inspurUserEntity.setGender("m");
            inspurUserEntity.setName("xucongjie"+i);
            inspurUserEntity.setSeniority("4");
            inspurUserEntity.setDepartment_level1("SBG");
            inspurUserEntity.setDepartment_level2("big_data");
            inspurUserEntity.setDepartment_level3("data_analytics");
            inspurUserEntity.setDepartment_leader("wangbingliang");
            fw.write(gson.toJson(inspurUserEntity)+"\n");
        }
        fw.flush();
        fw.close();
    }

}
