package jp.mericle.amazon_connect_real_time_streaming;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Optional;

import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.FrameProcessException;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadata;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor.FrameProcessor;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;

/**
 * 音声をお客様側とオペレーター側に分割して録音します。
 * @author Bladean Mericle
 */
public class AudioRecordFrameProcessor implements FrameProcessor {

    /**
     * お客様側のトラック名。
     */
    private static final String CUSTOMER_TRACK_NAME = "AUDIO_FROM_CUSTOMER";

    /**
     * オペレーター側のトラック名。
     */
    private static final String OPERATOR_TRACK_NAME = "AUDIO_TO_CUSTOMER";

    /**
     * 保存先のフォルダ。
     */
    private final String audioPath;

    /**
     * ストリーム情報。
     */
    private final VideoStreamData videoStreamData;

    /**
     * ウインドウ。
     */
    private final Window window;

    /**
     * 問い合わせの描画パネル。
     */
    private final ContactPanel contactPanel;

    /**
     * お客様側の出力ストリーム。
     */
    private final ByteArrayOutputStream customerStream = new ByteArrayOutputStream();

    /**
     * オペレーター側の出力ストリーム。
     */
    private final ByteArrayOutputStream operatorStream = new ByteArrayOutputStream();

    /**
     * コンストラクタ。
     * @param audioPath 保存先のフォルダ
     * @param videoStreamData ストリーム情報
     * @param window ウインドウ
     */
    public AudioRecordFrameProcessor(
            final String audioPath,
            final VideoStreamData videoStreamData,
            final Window window) {
        if (audioPath == null || audioPath.isEmpty())
        {
            throw new IllegalArgumentException("audioPath can't set null or empty.");
        }

        if (videoStreamData == null) {
            throw new IllegalArgumentException("videoStreamData can't set null.");
        }

        if (window == null) {
            throw new IllegalArgumentException("window can't set null.");
        }

        this.audioPath = audioPath;
        this.videoStreamData = videoStreamData;
        this.window = window;

        // 問い合わせの描画パネルを作成します。
        contactPanel = window.addContactPanel(videoStreamData);
    }

    /**
     * フレームを処理します。
     * @param frame フレーム
     * @param trackMetadata トラックメタ情報
     * @param fragmentMetadata フラグメントメタ情報
     */
    @Override
    public void process(
            final Frame frame,
            final MkvTrackMetadata trackMetadata,
            final Optional<FragmentMetadata> fragmentMetadata) throws FrameProcessException {
        process(frame, trackMetadata, fragmentMetadata, Optional.ofNullable(null));
    }

    /**
     * フレームを処理します。
     * @param frame フレーム
     * @param trackMetadata トラックメタ情報
     * @param fragmentMetadata フラグメントメタ情報
     * @param tagProcessor タグ処理
     */
    @Override
    public void process(
            final Frame frame,
            final MkvTrackMetadata trackMetadata,
            final Optional<FragmentMetadata> fragmentMetadata,
            final Optional<FragmentMetadataVisitor.MkvTagProcessor> tagProcessor) throws FrameProcessException {
        try {
            final ByteBuffer frameData = frame.getFrameData();
            byte[] frameBytes = new byte[frameData.remaining()];
            frameData.get(frameBytes);
            final String trackName = trackMetadata.getTrackName();

            // トラック名でどちら側の音声なのか判別します。
            if (trackName.equals(CUSTOMER_TRACK_NAME)) {
                customerStream.write(frameBytes);
                contactPanel.updateCustomerFrequencySpectrum(frameBytes);
            } else if (trackName.equals(OPERATOR_TRACK_NAME)) {
                operatorStream.write(frameBytes);
                contactPanel.updateOperatorFrequencySpectrum(frameBytes);
            }
        } catch (IOException e) {
            throw new FrameProcessException("Failed to write audio file.", e);
        }
    }

    /**
     * フレームの処理を終了します。
     */
    @Override
    public void close() {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
        final String baseFileName = dateFormat.format(videoStreamData.getStartTimestamp());

        // フォルダの作成
        new File(audioPath).mkdirs();

        final File customerPath = new File(audioPath, baseFileName + "-cu.wav");
        try (OutputStream customerFileStream = new FileOutputStream(customerPath, false)){
            WriteAudioData(customerFileStream, customerStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            customerStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        final File operatorPath = new File(audioPath, baseFileName + "-op.wav");
        try (OutputStream operatorFileStream = new FileOutputStream(operatorPath, false)){
            WriteAudioData(operatorFileStream, operatorStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            operatorStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 問い合わせの描画パネルを削除します。
        window.removeContactPanel(videoStreamData);
    }

    /**
     * 音声データを書き込みます。
     * http://soundfile.sapp.org/doc/WaveFormat/
     * @param outputStream 出力ストリーム
     * @param audioData 音声データ
     * @throws IOException 書き込みエラー
     */
    private static void WriteAudioData(final OutputStream outputStream, final byte[] audioData) throws IOException
    {
        final int audioLength = audioData.length;
        final Charset charset = StandardCharsets.ISO_8859_1;
        final ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;

        // チャンクの書き込み
        outputStream.write("RIFF".getBytes(charset));
        outputStream.write(ByteBuffer.allocate(4).order(byteOrder).putInt(36 + audioLength).array()); // チャンクのサイズ
        outputStream.write("WAVE".getBytes(charset));

        // サブチャンク1の書き込み
        outputStream.write("fmt ".getBytes(charset));
        outputStream.write(ByteBuffer.allocate(4).order(byteOrder).putInt(16).array()); // サブチャンク1のサイズ
        outputStream.write(ByteBuffer.allocate(2).order(byteOrder).putShort((short)1).array()); // PCM
        outputStream.write(ByteBuffer.allocate(2).order(byteOrder).putShort((short)1).array()); // モノラル
        outputStream.write(ByteBuffer.allocate(4).order(byteOrder).putInt(8000).array()); // 8kHz
        outputStream.write(ByteBuffer.allocate(4).order(byteOrder).putInt(16000).array()); // 8kHz * 1ch * 16bit / 8
        outputStream.write(ByteBuffer.allocate(2).order(byteOrder).putShort((short)2).array()); // 1ch * 16bit / 8
        outputStream.write(ByteBuffer.allocate(2).order(byteOrder).putShort((short)16).array()); // 16bit

        // サブチャンク2の書き込み
        outputStream.write("data".getBytes(charset));
        outputStream.write(ByteBuffer.allocate(4).order(byteOrder).putInt(audioLength).array()); // サブチャンク2のサイズ
        outputStream.write(audioData);
    }
}
