/**
 * @author Mr.lu
 * @Title: MySparkSession
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/17:10:06
 */

/**
 * 被构建者对象--测试构建者模式
 */
public class MySparkSession {

    public static void main(String[] args) {
        MySparkSession.builder()
                .setParam1("q")
                .setParam2("w")
                .setParam3("e")
                .getOrCreate();
    }

    /**
     * 被构造者参数
     */
    private String param1;
    private String param2;
    private String param3;
    private String param4;
    private String param5;

    /**
     * 5.返回：MySparkSessionBuild
     * @return
     */
    public static MySparkSessionBuild builder(){
        return new MySparkSessionBuild();
    }

    /**
     * 2.创建静态内部类
     */
    public static class MySparkSessionBuild{
        /**
         * 3.被构造者内部对象
         */

        private MySparkSession mySparkSession =new MySparkSession();

        /**
         *4.被构造的set方法，切方法返回自身对象
         * @param param1
         * @return
         */
        public MySparkSessionBuild setParam1(String param1){
            mySparkSession.setParam1(param1);
            //this--MySaprkSessionBuild:返回自身对象
            return this;
        }

        public MySparkSessionBuild setParam2(String param2){
            mySparkSession.setParam1(param2);
            return this;
        }
        public MySparkSessionBuild setParam3(String param3){
            mySparkSession.setParam1(param3);
            return this;
        }
        public MySparkSessionBuild setParam4(String param4){
            mySparkSession.setParam1(param4);
            return this;
        }
        public MySparkSessionBuild setParam5(String param5){
            mySparkSession.setParam1(param5);
            return this;
        }

        /**
         * 4.用来返回构造对象
         * @return
         */
        public MySparkSession getOrCreate(){
            return mySparkSession;
        }
    }

    public String getParam1() {
        return param1;
    }

    public void setParam1(String param1) {
        this.param1 = param1;
    }

    public String getParam2() {
        return param2;
    }

    public void setParam2(String param2) {
        this.param2 = param2;
    }

    public String getParam3() {
        return param3;
    }

    public void setParam3(String param3) {
        this.param3 = param3;
    }

    public String getParam4() {
        return param4;
    }

    public void setParam4(String param4) {
        this.param4 = param4;
    }

    public String getParam5() {
        return param5;
    }

    public void setParam5(String param5) {
        this.param5 = param5;
    }

}
