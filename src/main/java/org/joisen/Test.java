package org.joisen;

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

    public static void main(String[] args) {
        double res = champagneTower(100000009, 33, 17);
        System.out.println( res );
    }
}
