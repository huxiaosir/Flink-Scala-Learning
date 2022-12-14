package org.joisen;

import java.util.*;

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

    public static int beautySum(String s) {
        String str = "aabcb"; // len = 5;


        int res = 0;
        if(s.length() <= 3) return 0;
        for(int i=0;i<=s.length() - 3;i++){
            for(int j = i+3;j<=s.length();j++){
                System.out.println(s.substring(i, j) + " ---> " + beauty(s.substring(i,j) ));
                res += beauty(s.substring(i,j));
            }
        }
        return res;
    }

    public static int beauty(String s){
        int[] cnt = new int[26];
        int max = 1, min=Integer.MAX_VALUE;
        for(int i = 0; i < s.length(); i++){
            cnt[s.charAt(i) - 'a']++;
            max = Math.max(cnt[s.charAt(i)-'a'], max);
        }
        for(int i = 0; i< 26;i++){
            if(cnt[i] != 0){
                min = Math.min(cnt[i], min);
            }
        }
        return max - min;
    }

    public static int getSum(int i){
        int res = 0;
        while(i != 0){
            res += i % 10;
            i = i/10;
        }
        return res;
    }
    public static String minNumber(int[] nums) {
        List<List<Integer>> res = new ArrayList<>();
        List<Integer> tmp = new ArrayList<>();
        boolean[] visited = new boolean[nums.length];
        Arrays.sort(nums); // 排序
        backTrack(res,tmp,nums,0,visited);

        String ret = String.valueOf(Double.MAX_VALUE);
        for(List<Integer> tp: res){
            System.out.println(tp);
            StringBuilder sb = new StringBuilder();
            for(int num: tp){
                sb.append(num);
            }
            System.out.println("sb:::"+sb.toString());
            if(Double.parseDouble(ret) > Double.parseDouble(sb.toString()))
                ret = sb.toString();
        }
        return ret;
    }
    public static void backTrack(List<List<Integer>> res,List<Integer> tmp,int[] nums,int idx,boolean[] visited){
        if(idx == nums.length){
            res.add(new ArrayList<>(tmp));
            return;
        }
        for(int i =0;i < nums.length;i++){
            if(visited[i]){
                continue;
            }
            tmp.add(nums[i]);
            visited[i] = true;
            backTrack(res,tmp,nums,idx+1,visited);
            visited[i] = false;
            tmp.remove(idx);
        }
    }

    public static void main(String[] args) {
//        double res = champagneTower(100000009, 33, 17);
//        System.out.println( res );

//        String s = "10010100";
//        int res = minOperations(s);

//        int[] res = new int[]{5,5,34,45,2,6,8,3,8,9,60,1,34,52};
//        int[] arr = new int[]{1,2,3,4,5,6,7,8,9};
//        System.out.println(Arrays.binarySearch(arr, 100));
//        System.out.println(Arrays.stream(res).min().getAsInt());
//        int[] a = new int[]{1,2,3,4,5};
//        int[] constructArr = constructArr(a);

//        String s = "xzvfsppsjfbxdwkqe";
//        System.out.println("----------" + beautySum(s) + "----------");
//        int[] nums = new int[]{3,30,34,5,9};
        int[] nums = new int[]{999999998,999999997,999999999};
        String s = minNumber(nums);
        System.out.println(s);

    }
    public static int[] constructArr(int[] a) {
        int n = a.length;
        int[] B = new int[n];
        for (int i = 0, product = 1; i < n; product *= a[i], i++)       /* 从左往右累乘 */
            B[i] = product;
        for (int i : B) {
            System.out.print(i + "--");
        }
        System.out.println();
        for (int i = n - 1, product = 1; i >= 0; product *= a[i], i--)  /* 从右往左累乘 */
            B[i] *= product;
        for (int i : B) {
            System.out.print(i + "--");
        }
        return B;
    }
}


