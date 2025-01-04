package com.lartimes.unicom.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 13:55
 */
@Service
public class HDFSUtils {
    private final FileSystem fileSystem;

    public HDFSUtils(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    //    上传 下载
//     mysql<-->hdfs 这里不采用长连接tcp ， 采用ssh 命令
//    备份 archive 压缩等
    public void uploadFile(String localFilePath, String remoteFilePath) {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try {
            fileSystem.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void downloadFile(String localFilePath, String remoteFilePath) {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try {
            fileSystem.copyToLocalFile(false, remotePath, localPath, true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
//    TODO mapreduce 导入导出mysql
//    TODO 进行backup

}
