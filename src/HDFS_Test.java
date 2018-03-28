import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
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
    private static void multiWordCount(Configuration conf, String remoteFilePath, String dstFilePath) throws IOException {
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

                // 遍历HashMap,输出结果，并把结果上传到云端
                FileSystem fsDst = FileSystem.get(URI.create(dstFilePath), conf);
                FSDataOutputStream fsDataOutputStream = fsDst.create(new Path(dstFilePath));
                for (String key : hashMap.keySet()) {
                    System.out.println("key:"+key+"\t"+"values:"+hashMap.get(key));
                    fsDataOutputStream.writeBytes("key:"+key+"\t"+"values:"+hashMap.get(key));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("==========词频统计==========");
    }

    /**
     * 合并文件
     * 过滤：含有word的文件
     */
    private static void doMerge(Configuration conf, String remoteDir, String dstFilePath) throws IOException {
        System.out.println("==========合并文件==========");
        // 要合并的文件所在的HDFS路径
        FileSystem fsSource = FileSystem.get(URI.create(remoteDir), conf);
        // 合并后的 目的文件 路径
        FileSystem fsDst = FileSystem.get(URI.create(dstFilePath), conf);
        // 过滤掉输入目录中后缀为.abc的文件
        FileStatus[] sourceStatus = fsSource.listStatus(new Path(remoteDir), new MyPathFilter(".*\\abc"));
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
            }
        }
        fsDataOutputStream.close();
        System.out.println("合并文件到:" + dstFilePath);
        System.out.println("==========合并文件==========");
    }

    /**
     * 合并含word的文件，并统计词频
     * 用法：doMergeAndWordCount(conf,remoteDir,dstFilePath);
     *
     * @param conf        配置
     * @param remoteDir   HDFS目录
     * @param dstFilePath 目的文件
     * @throws IOException IOException
     */
    private static void doMergeAndWordCount(Configuration conf, String remoteDir, String dstFilePath) throws IOException {
        doMerge(conf, remoteDir, dstFilePath);
        multiWordCount(conf, dstFilePath, dstFilePath);
    }

    /**
     * 下载文件到本地
     * 判断本地路径是否已经存在,若已存在,则进行重命名
     */
    private static void downloadFromRemote(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        File f = new File(localFilePath);
        /* 如果文件名存在，自动重命名(在文件名后面加上 _0, _1 ...) */
        if (f.exists()) {
            System.out.println(localFilePath + " 已存在.");
            Integer i = 0;
            while (true) {
                f = new File(localFilePath + "_" + i.toString());
                if (!f.exists()) {
                    localFilePath = localFilePath + "_" + i.toString();
                    break;
                }
            }
            System.out.println("将重新命名为: " + localFilePath);
        }

        // 下载文件到本地
        Path localPath = new Path(localFilePath);
        fs.copyToLocalFile(remotePath, localPath);
        fs.close();
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
     * 显示HDFS中指定的文件夹下，所有文件的读写权限,大小,创建时间,路径
     * 用法：showHDFSInfo(conf,remoteDir);
     */
    private static void showHDFSInfo(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteDir);
        FileStatus[] fileStatuses = fs.listStatus(remotePath);
        for (FileStatus s : fileStatuses) {
            System.out.println("路径: " + s.getPath().toString());
            System.out.println("权限: " + s.getPermission().toString());
            System.out.println("大小: " + s.getLen());
            /* 返回的是时间戳,转化为时间日期格式 */
            Long timeStamp = s.getModificationTime();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = format.format(timeStamp);
            System.out.println("时间: " + date);
        }
        fs.close();
    }

    /**
     * 递归的显示
     */
    private static void showHDFSInfo_R(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        /* 递归获取目录下的所有文件 */
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
        /* 输出每个文件的信息 */
        while (remoteIterator.hasNext()) {
            FileStatus s = remoteIterator.next();
            System.out.println("路径: " + s.getPath().toString());
            System.out.println("权限: " + s.getPermission().toString());
            System.out.println("大小: " + s.getLen());
            /* 返回的是时间戳,转化为时间日期格式 */
            Long timeStamp = s.getModificationTime();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = format.format(timeStamp);
            System.out.println("时间: " + date);
            System.out.println();
        }
        fs.close();
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

        String remoteDir = "hdfs://localhost:9000/user/hadoop/test"; // 要合并的文件所在的HDFS路径
        String dstFilePath = "hdfs://localhost:9000/user/hadoop/output/output.txt";// 存到output/output.txt

        doMergeAndWordCount(conf,remoteDir,dstFilePath);
    }
}