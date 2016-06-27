package pt.tecnico.spark.util;

/**
 * Network statistic
 */
public class NetworkStatistic {
    private Integer stageId;
    private String stageName;
    private Double upload;
    private Double download;

    public NetworkStatistic(Integer stageId, String stageName, Double download, Double upload) {
        this.stageId = stageId;
        this.stageName = stageName;
        this.upload = upload;
        this.download = download;
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

    public Double getUpload() {
        return upload;
    }

    public void setUpload(Double upload) {
        this.upload = upload;
    }

    public Double getDownload() {
        return download;
    }

    public void setDownload(Double download) {
        this.download = download;
    }
}
