package pt.tecnico.spark.util;

/**
 * Created by dikei on 6/26/16.
 */
public class StageDiskStatistic {
    private Integer stageId;
    private String stageName;
    private Double diskRead;
    private Double diskWrite;

    public StageDiskStatistic(Integer stageId, String stageName, Double diskRead, Double diskWrite) {
        this.stageId = stageId;
        this.stageName = stageName;
        this.diskRead = diskRead;
        this.diskWrite = diskWrite;
    }

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public Double getDiskRead() {
        return diskRead;
    }

    public void setDiskRead(Double diskRead) {
        this.diskRead = diskRead;
    }

    public Double getDiskWrite() {
        return diskWrite;
    }

    public void setDiskWrite(Double diskWrite) {
        this.diskWrite = diskWrite;
    }
}
