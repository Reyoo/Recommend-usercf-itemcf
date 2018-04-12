package com.foriseholdings.algorithm.taskscheduling.model;


import java.util.Date;


public class Job {

	private Long id;

    private String jobName;

    private String busCode;

    private String parentCode;

    private String jobType;

    private Date jobTime;

    private String execState;

    private String prevJob;

    private String syncJob;

    private String nextJob;

    private Integer timeDelay;

    private Integer timespan;

    private String jobState;

    private String paramIds;

    private Date startTime;

    private Date endTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName == null ? null : jobName.trim();
    }

    public String getBusCode() {
        return busCode;
    }

    public void setBusCode(String busCode) {
        this.busCode = busCode == null ? null : busCode.trim();
    }

    public String getParentCode() {
        return parentCode;
    }

    public void setParentCode(String parentCode) {
        this.parentCode = parentCode == null ? null : parentCode.trim();
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType == null ? null : jobType.trim();
    }

    public Date getJobTime() {
        return jobTime;
    }

    public void setJobTime(Date jobTime) {
        this.jobTime = jobTime;
    }

    public String getExecState() {
        return execState;
    }

    public void setExecState(String execState) {
        this.execState = execState == null ? null : execState.trim();
    }

    public String getPrevJob() {
        return prevJob;
    }

    public void setPrevJob(String prevJob) {
        this.prevJob = prevJob == null ? null : prevJob.trim();
    }

    public String getSyncJob() {
        return syncJob;
    }

    public void setSyncJob(String syncJob) {
        this.syncJob = syncJob == null ? null : syncJob.trim();
    }

    public String getNextJob() {
        return nextJob;
    }

    public void setNextJob(String nextJob) {
        this.nextJob = nextJob == null ? null : nextJob.trim();
    }

    public Integer getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(Integer timeDelay) {
        this.timeDelay = timeDelay;
    }

    public Integer getTimespan() {
        return timespan;
    }

    public void setTimespan(Integer timespan) {
        this.timespan = timespan;
    }

    public String getJobState() {
        return jobState;
    }

    public void setJobState(String jobState) {
        this.jobState = jobState == null ? null : jobState.trim();
    }

    public String getParamIds() {
        return paramIds;
    }

    public void setParamIds(String paramIds) {
        this.paramIds = paramIds == null ? null : paramIds.trim();
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

}