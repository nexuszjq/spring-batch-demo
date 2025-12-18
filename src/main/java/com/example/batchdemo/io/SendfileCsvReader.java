package com.example.batchdemo.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 使用 sendfile（FileChannel.transferTo）将数据从文件通道推送到管道，
 * 由管道另一端流式消费，避免用户态缓冲区重复拷贝。
 */
public class SendfileCsvReader extends ItemStreamSupport implements ItemReader<String>, ItemStream {

    private final Path path;
    private final int chunkSize;

    private FileChannel channel;
    private Pipe.SourceChannel sourceChannel;
    private ByteBuffer readBuffer;
    private final StringBuilder lineBuffer = new StringBuilder();

    public SendfileCsvReader(Path path, int chunkSize) {
        this.path = path;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            Pipe pipe = Pipe.open();
            this.sourceChannel = pipe.source();
            // 启动后台线程负责使用 transferTo 推送数据到管道，保持零拷贝路径
            Thread transferThread = new Thread(() -> {
                try (Pipe.SinkChannel sink = pipe.sink()) {
                    long position = 0L;
                    long size = channel.size();
                    while (position < size) {
                        long transferred = channel.transferTo(position, chunkSize, sink);
                        if (transferred <= 0) {
                            break;
                        }
                        position += transferred;
                    }
                } catch (IOException ignored) {
                    // 关闭时可能触发异常，忽略即可
                }
            }, "sendfile-transfer");
            transferThread.start();
            this.readBuffer = ByteBuffer.allocate(chunkSize);
        } catch (IOException e) {
            throw new ItemStreamException("初始化 sendfile 读取器失败", e);
        }
    }

    @Nullable
    @Override
    public String read() throws Exception {
        while (true) {
            if (!fillBuffer()) {
                if (lineBuffer.length() > 0) {
                    String lastLine = lineBuffer.toString();
                    lineBuffer.setLength(0);
                    return lastLine;
                }
                return null;
            }
            readBuffer.flip();
            while (readBuffer.hasRemaining()) {
                byte b = readBuffer.get();
                if (b == '\n') {
                    String line = lineBuffer.toString();
                    lineBuffer.setLength(0);
                    return line;
                } else if (b != '\r') {
                    lineBuffer.append((char) b);
                }
            }
            readBuffer.clear();
        }
    }

    private boolean fillBuffer() throws IOException {
        int bytesRead = sourceChannel.read(readBuffer);
        return bytesRead >= 0 && readBuffer.position() > 0;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // 无需持久化状态
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (sourceChannel != null) {
                sourceChannel.close();
            }
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("关闭 sendfile 读取器失败", e);
        }
    }
}
