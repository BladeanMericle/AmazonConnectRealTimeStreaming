package jp.mericle.amazon_connect_real_time_streaming;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Path2D;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import org.jtransforms.fft.DoubleFFT_1D;

/**
 * 問い合わせの描画パネルです。
 * @author Bladean Mericle
 */
public class ContactPanel extends JPanel {

    /**
     * お客様側の周波数スペクトルの描画パネル。
     */
    private FrequencySpectrumCanvas customerPanel;

    /**
     * オペレーター側の周波数スペクトルの描画パネル。
     */
    private FrequencySpectrumCanvas operatorPanel;

    /**
     * コンストラクタ。
     * @param startDateTime 開始日時
     */
    public ContactPanel(Date startDateTime) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        JLabel startDateTimeLabel = new JLabel(dateFormat.format(startDateTime));
        startDateTimeLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        customerPanel = new FrequencySpectrumCanvas(new Color(0xFF, 0xA0, 0x7A));
        operatorPanel = new FrequencySpectrumCanvas(new Color(0x3C, 0xB3, 0x71));

        JLabel emptyLine = new JLabel(" ");
        emptyLine.setAlignmentX(Component.LEFT_ALIGNMENT);

        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
        add(startDateTimeLabel);
        add(customerPanel);
        add(operatorPanel);
        add(emptyLine);
    }

    /**
     * お客様側の周波数スペクトルを更新します。
     * @param data 音声データ
     */
    public void updateCustomerFrequencySpectrum(final byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        SwingUtilities.invokeLater(() -> {
            customerPanel.updateFrequencySpectrum(toFrequencySpectrum(data));
            customerPanel.revalidate();
            customerPanel.repaint();
        });
    }

    /**
     * オペレーター側の周波数スペクトルを更新します。
     * @param data 音声データ
     */
    public void updateOperatorFrequencySpectrum(final byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        SwingUtilities.invokeLater(() -> {
            operatorPanel.updateFrequencySpectrum(toFrequencySpectrum(data));
            operatorPanel.revalidate();
            operatorPanel.repaint();
        });
    }

    /**
     * 音声データを周波数スペクトルのデータに変換します。
     * https://stackoverflow.com/questions/7674877/how-to-get-frequency-from-fft-result
     * @param data 音声データ
     * @return 周波数スペクトルのデータ
     */
    private static double[] toFrequencySpectrum(final byte[] data) {
        double[] bufferData = new double[data.length];
        for (int i = 0; i < bufferData.length; ++i) {
            bufferData[i] = data[i];
        }

        DoubleFFT_1D fft = new DoubleFFT_1D(data.length);
        fft.realForward(bufferData);

        // 実数と虚数で取得するので半分にしています。
        // また変換したデータに対称性が見られたので、さらに半分にしています。(根拠なし)
        int length = data.length / 4;
        double[] processedData = new double[length];
        for (int i = 0; i < length; ++i) {
            double re = bufferData[2 * i];
            double im = bufferData[2 * i + 1];
            processedData[i] = Math.sqrt(re * re + im * im);
        }

        return processedData;
    }

    /**
     * 周波数スペクトルの描画パネルです。
     * @author Bladean Mericle
     */
    public class FrequencySpectrumCanvas extends JPanel {

        /**
         * 描画の幅。
         */
        private static final int width = 200;

        /**
         * 描画の高さ。
         */
        private static final int height = 40;

        /**
         * スペクトルの色。
         */
        private final Color spectrumColor;

        /**
         * 周波数スペクトルのデータ。
         */
        private double[] data;

        /**
         * コンストラクタ。
         * @param color スペクトルの色
         */
        public FrequencySpectrumCanvas(final Color color) {
            if (color == null) {
                throw new IllegalArgumentException("color can't set null.");
            }

            setAlignmentX(Component.LEFT_ALIGNMENT);
            setPreferredSize(new Dimension(width, height));
            spectrumColor = color;
        }

        /**
         * 描画処理。
         * @param g 描画オブジェクト。
         */
        public void paintComponent(final Graphics g) {
            if (data == null || data.length == 0) {
                g.setColor(Color.WHITE);
                g.fillRect(0, 0, width, height);
                return;
            }

            Graphics2D g2 = (Graphics2D)g;
            g2.setBackground(Color.WHITE);
            g2.clearRect(0, 0, width, height);
            g2.setColor(spectrumColor);
            Path2D.Double path = new Path2D.Double();
            path.moveTo(0, height);

            int length = data.length;
            for (int i = 0; i < length; ++i) {
                path.lineTo(
                        ((double)i / (double)length) * width,
                        height - ((Math.min(data[i], 8000.0) / 8000.0) * height)); // 適当な値でリミッターをかけています。
            }

            path.lineTo(width, height);
            path.closePath();
            g2.fill(path);
        }

        /**
         * 周波数スペクトルを更新します。
         * @param data 周波数スペクトルのデータ
         */
        public void updateFrequencySpectrum(final double[] data) {
            this.data = data;
        }
    }
}
