package com.zgl.hadoop;

import java.util.HashMap;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-8-10
 * \* Time: 16:16
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class TestMain {
    public static void main(String[] args){
       /* long l1 = System.currentTimeMillis();
        int sum=0;
        for(int i=0;i<=2000000000;i++){
            if(i%2==1){
             //if((i&1)==1){
                sum+=1;
            }
        }
        System.out.println(sum);
        long l2 = System.currentTimeMillis();
        System.out.println(l2-l1);*/
      // testHashMap();
        split();
    }

    public static void testHashMap(){
        HashMap hashMap = new HashMap<String,String>();
      //  System.out.println("threshold:"+hashMap.threshold);

        hashMap.put("001",1);
        hashMap.put("002",2);
        hashMap.put("003",3);
        hashMap.put("004",4);
        hashMap.put("005",5);
        hashMap.put("006",6);
        hashMap.put("007",7);
        hashMap.put("008",8);
        int oldvalue = (Integer) hashMap.put("005",9);
        System.out.println("oldValue:"+oldvalue+"newValue:"+hashMap.get("005"));
        System.out.println("size:"+hashMap.size());

    }

    public static void split(){
        String sql = "DROP TABLE `count`,`count_1`,`count_2`,`count_3`,`count_4`,`count_test`,`count_test1`,`count_test3`,`count_test4`,`count_test6`";

        String pas1 = "DROP TABLE IF EXISTS";
        String pas2 = "DROP TABLE";
        String strSplits = "1,2,3,4,5,6,7,8,9,A,B,C,D,E,F";
        String[] split = strSplits.split("\\,");
        String str = "120434107";
        System.out.println(split[(str.hashCode() & 0x7fffffff)%split.length]);
//        if(sql.contains(pas1)){
//            System.out.println(sql.substring(sql.indexOf(pas1),pas1.length()).trim());
//
//        }else if(sql.contains(pas2)){
//            System.out.println(sql.substring(sql.indexOf(pas2),pas2.length()).trim());
//        }



    }
}
