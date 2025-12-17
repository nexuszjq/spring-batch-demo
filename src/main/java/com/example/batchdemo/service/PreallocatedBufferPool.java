package com.example.batchdemo.service;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 在应用启动时预分配直接内存，避免每次读写大文件都临时申请内存。
 * 重启时会主动清空引用，帮助垃圾回收及时释放旧的直接内存，再重新分配。
 */
@Component
public class PreallocatedBufferPool {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int BUFFER_COUNT = 8; // 预分配 8 个缓冲区满足并发请求

    private final Deque<ByteBuffer> pool = new ArrayDeque<>();

    /**
     * 清空旧的直接内存引用并重新分配，确保内存可以复用。
     */
    public synchronized void flushAndPreallocate() {
        pool.clear();
        for (int i = 0; i < BUFFER_COUNT; i++) {
            pool.offer(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE));
        }
    }

    /**
     * 借出一个可重复使用的直接内存缓冲区。
     */
    public synchronized ByteBuffer borrowBuffer() {
        ByteBuffer buffer = pool.pollFirst();
        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        }
        buffer.clear();
        return buffer;
    }

    /**
     * 归还缓冲区，方便下一次请求继续复用。
     */
    public synchronized void returnBuffer(ByteBuffer buffer) {
        buffer.clear();
        pool.offerLast(buffer);
    }

    @PreDestroy
    public void onShutdown() {
        pool.clear();
    }
}
