import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

/**
 * @Author: skm
 * @Date: 2019/5/12 8:47
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class Test {
    private static String ClusterName = "mycluster";
    private static final String HADOOP_URL = "hdfs://" + ClusterName;
    public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        conf.set("dfs.ha.namenodes." + ClusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address." + ClusterName + ".nn1", "master:8020");
        conf.set("dfs.namenode.rpc-address." + ClusterName + ".nn2", "slave1:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider." + ClusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }
   @org.junit.Test
    public void st(){
        Date date = new Date();
        System.out.println(
                date
        );
    }
//    @org.junit.Test
//    public void test1(){
//        try {
//            Configuration conf = new Configuration();
//
//            Path path = new Path("hdsf://mycluster/dataBuider/web.log");
//            FSDataInputStream fsDataInputStream =
//            BufferedReader bufferedReader = new BufferedReader();
//            String line = null;
//            while ((line=bufferedReader.readLine()) !=null)){
//
//            }
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
    public void test2() throws Exception{
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("hdfs://mycluster/dataBuider/web.log");
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line = bufferedReader.readLine())!=null){

        }

    }
}
