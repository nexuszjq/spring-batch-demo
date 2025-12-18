package com.example.batchdemo;

import com.example.batchdemo.service.PreallocatedBufferPool;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource("classpath:batch/jobs.xml")
public class SpringBatchDemoApplication {

    private final PreallocatedBufferPool bufferPool;

    public SpringBatchDemoApplication(PreallocatedBufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchDemoApplication.class, args);
    }

    /**
     * 应用启动后预分配直接内存，确保重启后可以刷新上一次的缓存池，实现内存复用。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUpBuffers() {
        bufferPool.flushAndPreallocate();
    }
}
