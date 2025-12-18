package com.example.batchdemo.io;

import com.example.batchdemo.service.PreallocatedBufferPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ItemStreamSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 使用 FileChannel + 预分配直接内存进行写入，避免频繁创建缓冲区。
 */
public class FileChannelLineWriter extends ItemStreamSupport implements ItemWriter<String>, ItemStream {

    private final Path outputPath;
    private final PreallocatedBufferPool bufferPool;
    private FileChannel channel;

    public FileChannelLineWriter(Path outputPath, PreallocatedBufferPool bufferPool) {
        this.outputPath = outputPath;
        this.bufferPool = bufferPool;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            this.channel = FileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new ItemStreamException("打开输出文件失败", e);
        }
    }

    @Override
    public void write(List<? extends String> items) throws Exception {
        ByteBuffer buffer = bufferPool.borrowBuffer();
        try {
            for (String item : items) {
                byte[] bytes = (item + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
                if (buffer.remaining() < bytes.length) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    buffer.clear();
                }
                buffer.put(bytes);
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } finally {
            bufferPool.returnBuffer(buffer);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // 不需要额外状态
    }

    @Override
    public void close() throws ItemStreamException {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new ItemStreamException("关闭输出文件失败", e);
            }
        }
    }
}
