package com.example.batchdemo.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * 使用 RandomAccessFile 预留空间的方式添加文件头/尾，不需要全量读取大文件。
 */
@Service
public class RandomAccessHeaderFooterService {

    private static final int RESERVED_HEADER_SIZE = 256;

    /**
     * 创建文件并预留文件头空间，后续可直接覆盖写入 header 文本。
     */
    public void createFileWithReservedHeader(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(0);
            raf.seek(RESERVED_HEADER_SIZE);
        }
    }

    /**
     * 覆盖写入 header 内容，不需要移动正文数据。
     */
    public void writeHeader(Path path, String header) throws IOException {
        byte[] bytes = header.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > RESERVED_HEADER_SIZE) {
            throw new IllegalArgumentException("header 长度超出预留空间");
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek(0);
            raf.write(bytes);
        }
    }

    /**
     * 直接定位到文件末尾追加 footer，无需读取全量数据。
     */
    public void appendFooter(Path path, String footer) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(footer.getBytes(StandardCharsets.UTF_8));
        }
    }
}
