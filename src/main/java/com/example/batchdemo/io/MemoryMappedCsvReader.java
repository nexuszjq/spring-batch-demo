package com.example.batchdemo.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 使用 mmap 零拷贝技术按窗口映射 CSV 文件，逐行读取，避免一次性读入内存。
 */
public class MemoryMappedCsvReader extends ItemStreamSupport implements ItemReader<String>, ItemStream {

    private final Path path;
    private final int windowSize;

    private FileChannel channel;
    private long filePosition = 0L;
    private long fileSize = 0L;
    private MappedByteBuffer window;
    private final StringBuilder lineBuffer = new StringBuilder();

    public MemoryMappedCsvReader(Path path, int windowSize) {
        this.path = path;
        this.windowSize = windowSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.fileSize = channel.size();
            mapNextWindow();
        } catch (IOException e) {
            throw new ItemStreamException("无法打开文件进行内存映射", e);
        }
    }

    @Nullable
    @Override
    public String read() throws Exception {
        while (true) {
            if (window == null || !window.hasRemaining()) {
                if (filePosition >= fileSize) {
                    if (lineBuffer.length() > 0) {
                        String lastLine = lineBuffer.toString();
                        lineBuffer.setLength(0);
                        return lastLine;
                    }
                    return null;
                }
                mapNextWindow();
            }

            byte b = window.get();
            if (b == '\n') {
                String line = lineBuffer.toString();
                lineBuffer.setLength(0);
                return line;
            } else if (b != '\r') { // 忽略 Windows 换行的回车符
                lineBuffer.append((char) b);
            }
        }
    }

    private void mapNextWindow() throws IOException {
        long remaining = fileSize - filePosition;
        long size = Math.min(remaining, windowSize);
        if (size <= 0) {
            window = null;
            return;
        }
        window = channel.map(FileChannel.MapMode.READ_ONLY, filePosition, size);
        filePosition += size;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // 对于流式读取，不需要额外的状态保存
    }

    @Override
    public void close() throws ItemStreamException {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new ItemStreamException("关闭文件通道失败", e);
            }
        }
    }
}
