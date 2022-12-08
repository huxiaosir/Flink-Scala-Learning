package org.joisen;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @Author Joisen
 * @Date 2022/11/21 11:31
 * @Version 1.0
 */
public class Test {
    public static double champagneTower(int poured, int query_row, int query_glass) {
        double[] row = {poured}; // 初始只有一行，装着所有的香槟
        for (int i = 1; i <= query_row; i++) {
            double[] nextRow = new double[i + 1]; // nextRow表示下一行杯子
            for (int j = 0; j < i; j++) { // 遍历当前行的所有杯子的香槟
                double volume = row[j]; // 拿出当前杯子的香槟
                if (volume > 1) { // 当前杯子的总的香槟-1后均分给他左下杯子和右下杯子
                    nextRow[j] += (volume - 1) / 2;
                    nextRow[j + 1] += (volume - 1) / 2;
                }
            }
            row = nextRow; // 定位到下一行
        }
        return Math.min(1, row[query_glass]);
    }
    public static int minOperations(String s) {
        // // 经过变换，s最终可以变成两种类型的交替字符串：以0开头和以1开头的两种，两种变换的最少次数之和为s的长度
        // int res = 0;
        // if(s.length() == 1) return res;
        // for(int i = 0; i < s.length(); i++){
        //     char cur = s.charAt(i);
        //     if(cur != (char)('0' + i%2)) res ++;
        // }
        // return Math.min(res, s.length()-res);


        // 解法二：
        int res = 0;
        char[] sc = s.toCharArray();
        char pre = sc[0];
        for(int i=1;i<sc.length;i++){
            if(pre == sc[i]){
                res ++;
                pre = (char)(pre ^ 1);
                System.out.println(pre);
            }else{
                pre = sc[i];
            }
        }
        return Math.min(res, sc.length-res);
    }

    public char firstUniqChar(String s) {

        if("".equals(s)) return ' ';

        char[] arr = s.toCharArray();
        int[] side = new int[26];
        for(int i = 0; i < arr.length; i++){
            int val = arr[i] - 'a';
            side[val] ++;
        }
        for(char c: arr){
            if(side[c - 'a'] == 1) return c;
        }
        return ' ';
    }

    public static void main(String[] args) {
//        double res = champagneTower(100000009, 33, 17);
//        System.out.println( res );

//        String s = "10010100";
//        int res = minOperations(s);

        int[] res = new int[]{5,5,34,45,2,6,8,3,8,9,60,1,34,52};
        int[] arr = new int[]{1,2,3,4,5,6,7,8,9};
        System.out.println(Arrays.binarySearch(arr, 100));
//        System.out.println(Arrays.stream(res).min().getAsInt());


    }
}


