package com.example.batchdemo.service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * 每个文件处理请求对应一次 Job 启动，确保“一个文件一个请求”的语义。
 */
@Service
public class FileJobLauncherService {

    private final JobLauncher jobLauncher;
    private final Job memoryMappedJob;
    private final Job sendfileJob;

    public FileJobLauncherService(JobLauncher jobLauncher, Job memoryMappedJob, Job sendfileJob) {
        this.jobLauncher = jobLauncher;
        this.memoryMappedJob = memoryMappedJob;
        this.sendfileJob = sendfileJob;
    }

    public void launchMemoryMapped(String input, String output) throws Exception {
        launchJob(memoryMappedJob, input, output);
    }

    public void launchSendfile(String input, String output) throws Exception {
        launchJob(sendfileJob, input, output);
    }

    private void launchJob(Job job, String input, String output) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("input", input)
                .addString("output", output)
                .addLong("timestamp", Instant.now().toEpochMilli())
                .toJobParameters();
        jobLauncher.run(job, params);
    }
}
