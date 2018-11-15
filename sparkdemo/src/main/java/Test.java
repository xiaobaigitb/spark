/**
 * @author Mr.lu
 * @Title: Test
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/14:17:58
 */
public class Test {
    public static void main(String[] args) {
        int a[]={1,5,87,3,45,87,34};
        selectSort(a);
    }

    //选择排序
    private static void selectSort(int[] a) {
        int minIndex=0;
        int tmp=0;
        //判断数组是否有效
        if (a.length==0||a==null){
            return;
        }
        for (int i = 0; i < a.length-1; i++) {
            minIndex=i;
            for (int j = i+1; j < a.length; j++) {
                if (a[j]<a[minIndex]){
                    minIndex=j;
                }
            }
            //如果最小值不是第一个，将其换位置
            if (minIndex!=i){
                tmp=a[i];
                a[i]=a[minIndex];
                tmp=a[i];
            }
        }

        for (int num :
                a) {
            System.out.println(num);
        }
    }
}
