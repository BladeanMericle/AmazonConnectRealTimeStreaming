package jp.mericle.amazon_connect_real_time_streaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.swing.SwingUtilities;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.ebml.ParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoClientBuilder;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMediaClientBuilder;
import com.amazonaws.services.kinesisvideo.model.APIName;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointResult;
import com.amazonaws.services.kinesisvideo.model.GetMediaRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaResult;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * メイン処理。
 * @author Bladean Mericle
 */
public class App
{
    /**
     * 設定ファイルの名前。
     */
    private static final String PROPERTIES_FILE_NAME = "properties.xml";

    /**
     * メインメソッドです。
     * @param args コマンドライン引数
     */
    public static void main(final String[] args) {
        Properties settings = getSettings(PROPERTIES_FILE_NAME);
        if (settings == null)
        {
            System.err.printf("\"%s\"が見つかりませんでした。\n", PROPERTIES_FILE_NAME);
            return;
        }

        // リージョンの設定です。
        final Regions region = Regions.fromName(settings.getProperty("regionname"));

        // GetRecords の引数です。
        final String streamName = settings.getProperty("streamname");

        // AWS リクエストの最大リトライ数です。
        final int maxRetryCount = Integer.parseInt(settings.getProperty("maxretrycount"));

        // AWS リクエストのリトライ間隔です。
        final int retryInterval = Integer.parseInt(settings.getProperty("retryinterval"));

        // GetRecords の実行間隔です。
        final int getRecordsInterval = Integer.parseInt(settings.getProperty("getrecordsinterval"));

        // 音声の保存先フォルダです。
        final String audioPath = settings.getProperty("audiopath");


        // 最適な認証情報プロバイダを選択して使用します。
        // 例えば環境変数で設定する場合は、"AWS_ACCESS_KEY_ID"と"AWS_SECRET_ACCESS_KEY"を設定してください。
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        // クライアント環境の設定を行います。
        // 例えばプロキシの設定などはここで行います。
        final ClientConfiguration config = new ClientConfigurationFactory().getConfig();

        // Kinesis Data Streamsクライアントの設定を行います。
        final AmazonKinesis dataStreams = AmazonKinesisClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credentialsProvider)
                .withClientConfiguration(config).build();

        Window window = new Window((w) -> {
            // シャードの一覧を取得します。
            final List<Shard> shards = getShards(
                    dataStreams, streamName, maxRetryCount, retryInterval);
            if (shards == null || shards.size() == 0)
            {
                return;
            }

            // 一番最初のシャードから、シャードイテレータを取得します。
            String shardIterator = getShardIterator(
                    dataStreams, streamName, shards.get(0), maxRetryCount, retryInterval);
            if (shardIterator == null || shardIterator.isEmpty())
            {
                return;
            }

            // レコードごとの処理を生成します。
            final ObjectMapper mapper = new ObjectMapper();
            final Consumer<Record> recordProcessing = createRecordProcessing(
                    region, credentialsProvider, config, mapper, audioPath, maxRetryCount, retryInterval, w);

            System.out.println("Kinesis Data Streamsからのデータの受信を開始します。");
            while (true)
            {
                if (w.isCompleted()) {
                    break;
                }

                // レコードの一覧を取得します。
                shardIterator = getRecords(
                        dataStreams,
                        shardIterator,
                        recordProcessing,
                        maxRetryCount,
                        retryInterval);
                if (shardIterator == null || shardIterator.isEmpty())
                {
                    break;
                }

                try {
                    Thread.sleep(getRecordsInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }

            System.out.println("Kinesis Data Streamsからのデータの受信を終了します。"); // これが出るのは異常系
        });
        SwingUtilities.invokeLater(window);
    }

    /**
     * 設定を取得します。
     * @param path 設定のファイルパス
     * @return 設定
     */
    private static Properties getSettings(final String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }

        try (InputStream inputStream = new FileInputStream(path)) {
            Properties settings = new Properties();
            settings.loadFromXML(inputStream);
            return settings;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * レコードごとの処理を生成します。
     * @param mapper JSONマッパー
     * @param videoStreams Kinesis Video Streamsのクライアント
     * @param videoStreamsMediaBuilder Kinesis Video Streams Mediaのクライアントビルダー
     * @param audioPath 音声の保存先フォルダ
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return レコードごとの処理
     */
    private static Consumer<Record> createRecordProcessing(
            final Regions region,
            final AWSCredentialsProvider credentialsProvider,
            final ClientConfiguration config,
            final ObjectMapper mapper,
            final String audioPath,
            final int maxRetryCount,
            final int retryInterval,
            final Window window) {
        // Kinesis Video Streamsクライアントの設定を行います。
        final AmazonKinesisVideo videoStreams = AmazonKinesisVideoClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credentialsProvider)
                .withClientConfiguration(config).build();
        return (record) -> {
            System.out.printf("データを受信しました。\n");
            try {
                final JsonNode node = mapper.readTree(record.getData().array());
                final VideoStreamData videoStreamData = getVideoStreamData(node);
                if (videoStreamData == null) {
                    return;
                }

                Thread thraed = new Thread(() -> {
                    final String dataEndPoint = getDataEndpoint(videoStreams, videoStreamData, maxRetryCount, retryInterval);
                    if (dataEndPoint == null || dataEndPoint.isEmpty()) {
                        return;
                    }

                    // Kinesis Video Streams Mediaクライアントの設定を行います。
                    final AmazonKinesisVideoMedia videoStreamsMedia = AmazonKinesisVideoMediaClientBuilder.standard()
                            .withCredentials(credentialsProvider)
                            .withClientConfiguration(config)
                            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                                    dataEndPoint,
                                    region.getName())).build();

                    try (InputStream payload = getMedia(videoStreamsMedia, videoStreamData, maxRetryCount, retryInterval);
                            AudioRecordFrameProcessor frameProcessor = new AudioRecordFrameProcessor(audioPath, videoStreamData, window)){
                        if (payload == null) {
                            return;
                        }

                        ParserByteSource byteSource = new InputStreamParserByteSource(payload);
                        FrameVisitor visitor = FrameVisitor.create(frameProcessor);
                        StreamingMkvReader reader = StreamingMkvReader.createDefault(byteSource);

                        // 音声の取得中はここで処理が止まるので、別スレッドで処理しています。
                        System.out.printf("録音を開始します。\n");
                        reader.apply(visitor);
                        System.out.printf("録音を終了します。\n");
                    } catch (MkvElementVisitException | IOException e) {
                        e.printStackTrace();
                    }
                });
                thraed.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    /**
     * シャードの一覧を取得します。
     * https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html
     * @param dataStreams Kinesis Data Streams のクライアント
     * @param streamName ストリーム名
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return シャードの一覧
     */
    private static List<Shard> getShards(
            final AmazonKinesis dataStreams,
            final String streamName,
            final int maxRetryCount,
            final int retryInterval) {
        if (dataStreams == null) {
            throw new IllegalArgumentException("dataStreams can't set null.");
        }

        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("streamName can't set null or empty.");
        }

        return runAwsRequest(
                () -> {
                    final DescribeStreamRequest request = new DescribeStreamRequest()
                            .withStreamName(streamName);
                    final DescribeStreamResult result = dataStreams.describeStream(request);
                    return result.getStreamDescription().getShards();
                },
                maxRetryCount,
                retryInterval);
    }

    /**
     * シャードイテレータを取得します。
     * https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
     * @param dataStreams Kinesis Data Streams のクライアント
     * @param shard シャード
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return シャードイテレータ
     */
    private static String getShardIterator(
            final AmazonKinesis dataStreams,
            final String streamName,
            final Shard shard,
            final int maxRetryCount,
            final int retryInterval) {
        if (dataStreams == null) {
            throw new IllegalArgumentException("dataStreams can't set null.");
        }

        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("streamName can't set null or empty.");
        }

        if (shard == null) {
            throw new IllegalArgumentException("shard can't set null.");
        }

        return runAwsRequest(
                () -> {
                    final GetShardIteratorRequest request = new GetShardIteratorRequest()
                            .withStreamName(streamName)
                            .withShardId(shard.getShardId())
                            .withShardIteratorType(ShardIteratorType.LATEST);
                    final GetShardIteratorResult result = dataStreams.getShardIterator(request);
                    return result.getShardIterator();
                },
                maxRetryCount,
                retryInterval);
    }

    /**
     * レコードの一覧を取得します。
     * https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
     * @param dataStreams Kinesis Data Streams のクライアント
     * @param shardIterator シャードイテレータ
     * @param recordProcessing レコードの処理
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return 次のシャードイテレータ
     */
    private static String getRecords(
            final AmazonKinesis dataStreams,
            final String shardIterator,
            final Consumer<Record> recordProcessing,
            final int maxRetryCount,
            final int retryInterval) {
        if (dataStreams == null) {
            throw new IllegalArgumentException("dataStreams can't set null.");
        }

        if (shardIterator == null || shardIterator.isEmpty()) {
            throw new IllegalArgumentException("shardIterator can't set null or empty.");
        }

        if (recordProcessing == null) {
            throw new IllegalArgumentException("recordProcessing can't set null.");
        }

        return runAwsRequest(
                () -> {
                    final GetRecordsRequest request = new GetRecordsRequest()
                            .withShardIterator(shardIterator);
                    final GetRecordsResult result = dataStreams.getRecords(request);
                    final List<Record> records = result.getRecords();
                    for (Record record : records) {
                        recordProcessing.accept(record);
                    }

                    return result.getNextShardIterator();
                },
                maxRetryCount,
                retryInterval);
    }

    /**
     * JSONノードからストリーム名を取得します。
     * https://docs.aws.amazon.com/ja_jp/general/latest/gr/aws-arns-and-namespaces.html
     * @param node JSONノード
     * @return ストリーム名
     */
    private static VideoStreamData getVideoStreamData(final JsonNode node) {
        final JsonNode audioNode = node
                .path("Details")
                .path("ContactData")
                .path("MediaStreams")
                .path("Customer")
                .path("Audio");

        final String streamArn = audioNode
                .path("StreamARN")
                .asText();
        if (streamArn == null || streamArn.isEmpty()) {
            System.out.printf("Not found StreamARN from JSON.");
            return null;
        }

        // Amazon Kinesis Video Stream の ARN の書式は以下の通りです。
        // arn:aws:kinesisvideo:region:account-id:application/stream-name/code
        // つまり、スラッシュで分割した2番目の要素がストリーム名となります。
        String[] streamArnParts = streamArn.split("/");
        if (streamArnParts.length < 2) {
            System.out.printf("Not found stream name from StreamARN.");
            return null;
        }

        final String streamName = streamArnParts[1];
        if (streamName == null || streamName.isEmpty()) {
            System.out.printf("Not found stream name from StreamARN.");
            return null;
        }

        final Date startTimestamp = new Date(audioNode
                .path("StartTimestamp")
                .asLong());
        return new VideoStreamData(streamName, startTimestamp);
    }

    /**
     * GetMediaのエンドポイントを取得します。
     * https://docs.aws.amazon.com/kinesisvideostreams/latest/dg/API_GetDataEndpoint.html
     * @param videoStreams Kinesis Video Streamsのクライアント
     * @param videoStreamData ストリーム情報
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return GetMediaのエンドポイント
     */
    private static String getDataEndpoint(
            final AmazonKinesisVideo videoStreams,
            final VideoStreamData videoStreamData,
            final int maxRetryCount,
            final int retryInterval) {
        if (videoStreams == null) {
            throw new IllegalArgumentException("videoStreams can't set null.");
        }

        if (videoStreamData == null) {
            throw new IllegalArgumentException("videoStreamData can't set null.");
        }

        return runAwsRequest(
                () -> {
                    final GetDataEndpointRequest request = new GetDataEndpointRequest()
                            .withAPIName(APIName.GET_MEDIA)
                            .withStreamName(videoStreamData.getStreamName());
                    final GetDataEndpointResult result = videoStreams.getDataEndpoint(request);
                    return result.getDataEndpoint();
                },
                maxRetryCount,
                retryInterval);
    }

    /**
     * メディアの映像・音声を取得します。
     * https://docs.aws.amazon.com/kinesisvideostreams/latest/dg/API_dataplane_GetMedia.html
     * @param videoStreams Kinesis Video Streams Mediaのクライアント
     * @param videoStreamData ストリーム情報
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return メディアの映像・音声のペイロード
     */
    private static InputStream getMedia(
            final AmazonKinesisVideoMedia videoStreamsMedia,
            final VideoStreamData videoStreamData,
            final int maxRetryCount,
            final int retryInterval) {
        if (videoStreamsMedia == null) {
            throw new IllegalArgumentException("videoStreamsMedia can't set null.");
        }

        if (videoStreamData == null) {
            throw new IllegalArgumentException("videoStreamData can't set null.");
        }

        return runAwsRequest(
                () -> {
                    final StartSelector startSelector = new StartSelector()
                            .withStartSelectorType(StartSelectorType.SERVER_TIMESTAMP)
                            .withStartTimestamp(videoStreamData.getStartTimestamp());
                    final GetMediaRequest request = new GetMediaRequest()
                            .withStartSelector(startSelector)
                            .withStreamName(videoStreamData.getStreamName());
                    final GetMediaResult result = videoStreamsMedia.getMedia(request);
                    return result.getPayload();
                },
                maxRetryCount,
                retryInterval);
    }

    /**
     * AWS のリクエスト処理を実行します。
     * @param <T> 結果の型
     * @param requestFunction AWS のリクエスト処理
     * @param maxRetryCount 最大リトライ数
     * @param retryInterval リトライ間隔
     * @return リクエスト結果、処理に失敗した場合は{@code null}
     */
    private static <T> T runAwsRequest(
            final Supplier<T> requestProcessing,
            final int maxRetryCount,
            final int retryInterval) {
        if (requestProcessing == null) {
            throw new IllegalArgumentException("requestFunction can't set null.");
        }

        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("maxRetryCount can't set negative number.");
        }

        if (retryInterval < 0) {
            throw new IllegalArgumentException("retryInterval can't set negative number.");
        }

        for (int i = 0; i <= maxRetryCount; ++i) {
            try {
                return requestProcessing.get();
            } catch (AmazonServiceException e) {
                System.err.printf("[AmazonServiceException]\n");
                System.err.printf("     RequestId: %s\n", e.getRequestId());
                System.err.printf("    StatusCode: %d\n", e.getStatusCode());
                System.err.printf("     ErrorType: %s\n", e.getErrorType());
                System.err.printf("     ErrorCode: %s\n", e.getErrorCode());
                System.err.printf("  ErrorMessage: %s\n", e.getErrorMessage());
                System.err.printf("   IsRetryable: %b\n", e.isRetryable());
                e.printStackTrace();
                if (!e.isRetryable())
                {
                    return null;
                }
            } catch (AmazonClientException e) {
                System.err.printf("[AmazonClientException]\n");
                System.err.printf("      Message: %s\n", e.getMessage());
                System.err.printf("  IsRetryable: %b\n", e.isRetryable());
                e.printStackTrace();
                if (!e.isRetryable())
                {
                    return null;
                }
            }

            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        return null;
    }
}
