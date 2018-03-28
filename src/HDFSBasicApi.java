import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class HDFSBasicApi {
    public static boolean test(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * 创建目录
     */
    public static boolean mkdir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        boolean result = fs.mkdirs(dirPath);
        fs.close();
        return result;
    }

    /**
     * 判断目录是否为空
     * true: 空，false: 非空
     */
    public static boolean isDirEmpty(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
        return !remoteIterator.hasNext();
    }

    /**
     * 删除目录
     */
    public static boolean rmDir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        /* 第二个参数表示是否递归删除所有文件 */
        boolean result = fs.delete(dirPath, true);
        fs.close();
        return result;
    }

    /**
     * 创建文件
     */
    public static void touchz(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataOutputStream outputStream = fs.create(remotePath);
        outputStream.close();
        fs.close();
    }

    /**
     * 删除文件
     */
    public static boolean rm(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        boolean result = fs.delete(remotePath, false);
        fs.close();
        return result;
    }

    /**
     * 移动文件
     */
    public static boolean mv(Configuration conf, String remoteFilePath, String remoteToFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(remoteFilePath);
        Path dstPath = new Path(remoteToFilePath);
        boolean result = fs.rename(srcPath, dstPath);
        fs.close();
        return result;
    }

//    /**
//     * 主函数
//     */
//    public static void main(String[] args) {
//        Configuration conf = new Configuration();
//        conf.set("fs.default.name","hdfs://localhost:9000");
//        String remoteFilePath = "/user/hadoop/input/text.txt";    // HDFS路径
//        String remoteDir = "/user/hadoop/input";    // HDFS路径对应的目录
//
//        try {
//            /* 判断路径是否存在，存在则删除，否则进行创建 */
//            if ( test(conf, remoteFilePath) ) {
//                rm(conf, remoteFilePath); // 删除
//                System.out.println("删除路径: " + remoteFilePath);
//            } else {
//                if ( !test(conf, remoteDir) ) { // 若目录不存在，则进行创建
//                    mkdir(conf, remoteDir);
//                    System.out.println("创建文件夹: " + remoteDir);
//                }
//                touchz(conf, remoteFilePath);
//                System.out.println("创建路径: " + remoteFilePath);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    /**
//     * 主函数
//     */
//    public static void main(String[] args) {
//        Configuration conf = new Configuration();
//        conf.set("fs.default.name","hdfs://localhost:9000");
//        String remoteDir = "/user/hadoop/input";    // HDFS目录
//        Boolean forceDelete = false;  // 是否强制删除
//
//        try {
//            /* 判断目录是否存在，不存在则创建，存在则删除 */
//            if ( !HDFSApi.test(conf, remoteDir) ) {
//                HDFSApi.mkdir(conf, remoteDir); // 创建目录
//                System.out.println("创建目录: " + remoteDir);
//            } else {
//                if ( HDFSApi.isDirEmpty(conf, remoteDir) || forceDelete ) { // 目录为空或强制删除
//                    HDFSApi.rmDir(conf, remoteDir);
//                    System.out.println("删除目录: " + remoteDir);
//                } else  { // 目录不为空
//                    System.out.println("目录不为空，不删除: " + remoteDir);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
