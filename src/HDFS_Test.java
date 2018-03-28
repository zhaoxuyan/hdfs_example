import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.StringTokenizer;

public class HDFS_Test {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * 判断路径是否存在
     */
    private static boolean test(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * 复制文件到指定路径
     * 若路径已存在，则进行覆盖
     */
    private static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        /* fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖 */
        fs.copyFromLocalFile(false, true, localPath, remotePath);
        fs.close();
    }

    /**
     * 追加文件内容
     */
    private static void appendToFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        /* 创建一个文件读入流 */
        FileInputStream in = new FileInputStream(localFilePath);
        /* 创建一个文件输出流，输出的内容将追加到文件末尾 */
        FSDataOutputStream out = fs.append(remotePath);
        /* 读写文件内容 */
        byte[] data = new byte[1024];
        int read;
        while ((read = in.read(data)) > 0) {
            out.write(data, 0, read);
        }
        out.close();
        in.close();
        fs.close();
    }

    /**
     * 统计某单词的数量
     * 用法：wordCount(conf,remoteFilePath,"hello")
     */
    private static void wordCount(Configuration conf, String remoteFilePath, String temp) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (test(conf, remoteFilePath)) {
            FSDataInputStream getIt = fs.open(new Path(remoteFilePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(getIt));
            String content;
            int count = 0;
            while ((content = reader.readLine()) != null) {
                String parts[];
                parts = content.split(" ");
                for (String string : parts) {
                    if (string.equals(temp)) {
                        count++;
                    }
                }
            }
            System.out.println(temp + "出现的次数是" + count);
            reader.close();
            fs.close();
        } else {
            System.out.println("文件不存在！");
            fs.close();
        }
    }

    /**
     * 统计词频
     * 用法:multiWordCount(conf,remoteFilePath)
     */
    private static void multiWordCount(Configuration conf, String remoteFilePath) throws IOException {
        System.out.println("==========词频统计==========");
        FileSystem fs = FileSystem.get(conf);
        if (test(conf, remoteFilePath)) {
            FSDataInputStream getIt = fs.open(new Path(remoteFilePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(getIt));
            String content;
            HashMap<String, Integer> hashMap = new HashMap<>();
            // 用正则表达式来过滤字符串中的所有标点符号(含中文)
            String regex = "[【】、.。,，\"!--;:?\'\\]]";
            try {
                // 读取要处理的文件
                while ((content = reader.readLine()) != null) {
                    content = content.replaceAll(regex, " ");
                    // 使用StringTokenizer来分词(StringTokenizer详见JDK文档)
                    StringTokenizer tokenizer = new StringTokenizer(content);
                    while (tokenizer.hasMoreTokens()) {
                        String key = tokenizer.nextToken();
                        if (!hashMap.containsKey(key)) {
                            hashMap.put(key, 1);
                        } else {
                            int value = hashMap.get(key) + 1;
                            hashMap.put(key, value);
                        }
                    }
                }
                // 遍历HashMap,输出结果
                for (String key : hashMap.keySet()) {
                    System.out.println(key + ":" + hashMap.get(key));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("==========词频统计==========");
    }

    /**
     * 输出HDFS中指定文件本文到终端中
     * 用法:catFromRemote(dstFilePath);
     */
    private static void catFromRemote(String remoteFilePath) {
        InputStream in = null;
        try {
            /* 通过URL对象打开数据流，从中读取数据 */
            in = new URL(remoteFilePath).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 合并文件
     * 过滤：含有word的文件
     */
    private static void doMerge(Configuration conf, String remoteDic, String dstFilePath) throws IOException {
        System.out.println("==========合并文件==========");
        // 要合并的文件所在的HDFS路径
        FileSystem fsSource = FileSystem.get(URI.create(remoteDic), conf);
        // 合并后的 目的文件 路径
        FileSystem fsDst = FileSystem.get(URI.create(dstFilePath), conf);
        // 过滤掉输入目录中后缀为.abc的文件
        FileStatus[] sourceStatus = fsSource.listStatus(new Path(remoteDic), new MyPathFilter(".*\\abc"));
        // 创建目的文件
        FSDataOutputStream fsDataOutputStream = fsDst.create(new Path(dstFilePath));
        // 下面分别读取过滤之后的每个文件的内容，并输出到同一个文件中
        for (FileStatus status : sourceStatus) {
            FSDataInputStream fsDataInputStream = fsSource.open(status.getPath());
            // 字符流
            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            // 标签：是否含有某字符串 默认为false
            boolean flag = false;
            String content;
            while ((content = reader.readLine()) != null) {
                String parts[];
                parts = content.split(" ");
                for (String string : parts) {
                    // 是否有word ????????
                    if (string.equals("word")) {
                        flag = true;
                        System.out.println("路径：" + status.getPath()
                                + "文件大小：" + status.getLen()
                                + "权限" + status.getPermission()
                                + "内容：");
                        break;
                    }
                }
            }
            fsDataInputStream.close();
            reader.close();
            if (flag) {
                FSDataInputStream fsDataInputStream1 = fsSource.open(status.getPath());
                // 字节流
                byte[] data = new byte[1024];
                int read = -1;
                PrintStream printStream = new PrintStream(System.out);
                while ((read = fsDataInputStream1.read(data)) > 0) {
                    printStream.write(data, 0, read);
                    fsDataOutputStream.write(data, 0, read);
                }
                fsDataInputStream.close();
                System.out.println("\n");
//                printStream.close();
            }
        }
        fsDataOutputStream.close();
        System.out.println("合并文件到:" + dstFilePath);
        System.out.println("==========合并文件==========");
    }

    /**
     * 主函数
     * 任务：将文件中有hello的文件整合打包，并统计词频
     */

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        String localFilePath = "/Users/zhaoxuyan/IdeaProjects/hdfs_example/word.txt";  // 本地路径
        String remoteFilePath = "/user/hadoop/test/test.txt";  // HDFS路径

        String remoteDic = "hdfs://localhost:9000/user/hadoop/test"; // 要合并的文件所在的HDFS路径
        String dstFilePath = "hdfs://localhost:9000/user/hadoop/output/output.txt";// 存到output/output.txt
//        String choice = "append";    // 若文件存在则追加到文件末尾
////      String choice = "overwrite";    // 若文件存在则覆盖
//
//        try {
//            /* 判断文件是否存在 */
//            Boolean fileExists = false;
//            if (test(conf, remoteFilePath)) {
//                fileExists = true;
//                System.out.println(remoteFilePath + " 已存在.");
//            } else {
//                System.out.println(remoteFilePath + " 不存在.");
//            }
//            /* 进行处理 */
//            if (!fileExists) { // 文件不存在，则上传
//                copyFromLocalFile(conf, localFilePath, remoteFilePath);
//                System.out.println(localFilePath + " 已上传至 " + remoteFilePath);
//            } else if (choice.equals("overwrite")) {    // 选择覆盖
//                copyFromLocalFile(conf, localFilePath, remoteFilePath);
//                System.out.println(localFilePath + " 已覆盖 " + remoteFilePath);
//            } else if (choice.equals("append")) {   // 选择追加
//                appendToFile(conf, localFilePath, remoteFilePath);
//                System.out.println(localFilePath + " 已追加至 " + remoteFilePath);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        doMerge(conf, remoteDic, dstFilePath);
        multiWordCount(conf, dstFilePath);
    }
}