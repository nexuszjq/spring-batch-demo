package com.example.batchdemo.config;

import com.example.batchdemo.io.FileChannelLineWriter;
import com.example.batchdemo.io.MemoryMappedCsvReader;
import com.example.batchdemo.io.SendfileCsvReader;
import com.example.batchdemo.job.DataCleaningProcessor;
import com.example.batchdemo.service.PreallocatedBufferPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.nio.file.Path;

@Configuration
@EnableBatchProcessing
public class BatchJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PreallocatedBufferPool bufferPool;

    public BatchJobConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager, PreallocatedBufferPool bufferPool) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.bufferPool = bufferPool;
    }

    /**
     * mmap 版本 Job：通过内存映射实现零拷贝读取。
     */
    @Bean
    public Job memoryMappedJob(Step memoryMappedStep) {
        return new JobBuilder("memoryMappedJob", jobRepository)
                .start(memoryMappedStep)
                .build();
    }

    /**
     * sendfile 版本 Job：通过 transferTo 传输到管道实现零拷贝读取。
     */
    @Bean
    public Job sendfileJob(Step sendfileStep) {
        return new JobBuilder("sendfileJob", jobRepository)
                .start(sendfileStep)
                .build();
    }

    @Bean
    public Step memoryMappedStep(@Value("#{jobParameters['input']}") String input,
                                 @Value("#{jobParameters['output']}") String output) {
        return new StepBuilder("memoryMappedStep", jobRepository)
                .<String, String>chunk(200)
                .reader(memoryMappedReader(input))
                .processor(defaultProcessor())
                .writer(writer(output))
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Step sendfileStep(@Value("#{jobParameters['input']}") String input,
                             @Value("#{jobParameters['output']}") String output) {
        return new StepBuilder("sendfileStep", jobRepository)
                .<String, String>chunk(200)
                .reader(sendfileReader(input))
                .processor(defaultProcessor())
                .writer(writer(output))
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<String> memoryMappedReader(@Value("#{jobParameters['input']}") String input) {
        return new MemoryMappedCsvReader(Path.of(input), 8 * 1024 * 1024); // 8MB 窗口
    }

    @Bean
    @StepScope
    public ItemReader<String> sendfileReader(@Value("#{jobParameters['input']}") String input) {
        return new SendfileCsvReader(Path.of(input), 4 * 1024 * 1024); // 4MB 分片大小
    }

    @Bean
    public ItemProcessor<String, String> defaultProcessor() {
        return new DataCleaningProcessor();
    }

    @Bean
    @StepScope
    public ItemWriter<String> writer(@Value("#{jobParameters['output']}") String output) {
        return new FileChannelLineWriter(Path.of(output), bufferPool);
    }
}
