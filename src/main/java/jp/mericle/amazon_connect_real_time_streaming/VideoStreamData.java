package jp.mericle.amazon_connect_real_time_streaming;

import java.util.Date;

/**
 * Contact flow eventから取得したKinesis Video Streamsのストリーム情報です。
 * @author Bladean Mericle
 */
public class VideoStreamData {

    /**
     * ストリーム名。
     */
    private final String streamName;

    /**
     * 開始時のタイムスタンプ。
     */
    private final Date startTimestamp;

    /**
     * コンストラクタ。
     * @param streamName ストリーム名
     * @param startTimestamp 開始時のタイムスタンプ
     */
    public VideoStreamData(
            final String streamName,
            final Date startTimestamp)
    {
        if (streamName == null || streamName.isEmpty())
        {
            throw new IllegalArgumentException("streamName can't set null or empty.");
        }

        if (startTimestamp == null)
        {
            throw new IllegalArgumentException("startTimestamp can't set null.");
        }

        this.streamName = streamName;
        this.startTimestamp = startTimestamp;
    }

    /**
     * ストリーム名を取得します。
     * @return ストリーム名
     */
    public String getStreamName()
    {
        return streamName;
    }

    /**
     * 開始時のタイムスタンプを取得します。
     * @return 開始時のタイムスタンプ
     */
    public Date getStartTimestamp()
    {
        return startTimestamp;
    }
}
