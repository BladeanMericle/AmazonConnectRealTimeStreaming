package jp.mericle.amazon_connect_real_time_streaming;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.HashMap;
import java.util.function.Consumer;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

/**
 * Amazon Connect Real Time Streamingのウインドウです。
 * @author Bladean Mericle
 */
public class Window implements Runnable {

    /**
     * 問い合わせパネルテーブル。
     * キーはKinesis Video Streamsのストリーム名です。
     */
    private final HashMap<String, ContactPanel> contactPanelTable = new HashMap<String, ContactPanel>();

    /**
     * ビジネスロジック。
     */
    private final Consumer<Window> businessLogic;

    /**
     * 問い合わせパネル。
     */
    private JPanel contactPanels;

    /**
     * 処理が完了したかどうか。
     */
    private volatile boolean isCompleted = false;;

    /**
     * コンストラクタ。
     * @param businessLogic ビジネスロジック
     */
    public Window(Consumer<Window> businessLogic) {
        this.businessLogic = businessLogic;
    }

    /**
     * ウインドウを作成します。
     */
    @Override
    public void run() {
        contactPanels = new JPanel();
        contactPanels.setLayout(new BoxLayout(contactPanels, BoxLayout.Y_AXIS));

        final JPanel scrollpanel = new JPanel();
        scrollpanel.add(contactPanels, BorderLayout.PAGE_START);

        final JScrollPane scrollpane = new JScrollPane(scrollpanel);
        scrollpane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        scrollpane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

        final JFrame frame = new JFrame();
        final Container container = frame.getContentPane();
        container.add(scrollpane);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setTitle("Amazon Connect Real Time Streaming");
        frame.setSize(400, 400);
        frame.setLocationRelativeTo(null); // 画面中央に表示

        if (businessLogic != null) {
            final Thread thread = new Thread(() -> {
                businessLogic.accept(this);
            });
            thread.start();
        }

        frame.addWindowListener(new WindowAdapter() {
            public void windowClosed(WindowEvent e) {
                isCompleted = true;
            }
        });
        frame.setVisible(true);
    }

    /**
     * 問い合わせパネルを追加します。
     * @param videoStreamData Kinesis Video Streamsのストリーム情報
     * @return 問い合わせパネル
     */
    public ContactPanel addContactPanel(final VideoStreamData videoStreamData) {
        if (videoStreamData == null) {
            return null;
        }

        synchronized(contactPanelTable) {
            final ContactPanel contactPanel = contactPanelTable.get(videoStreamData.getStreamName());
            if (contactPanel != null) {
                return contactPanel;
            }

            final ContactPanel newContactPanel = new ContactPanel(videoStreamData.getStartTimestamp());
            contactPanelTable.put(videoStreamData.getStreamName(), newContactPanel);

            SwingUtilities.invokeLater(() -> {
                contactPanels.add(newContactPanel);
                contactPanels.revalidate();
                contactPanels.repaint();
            });
            return newContactPanel;
        }
    }

    /**
     * 問い合わせパネルを削除します。
     * @param streamName Kinesis Video Streamsのストリーム情報
     * @return 問い合わせパネル
     */
    public ContactPanel removeContactPanel(final VideoStreamData videoStreamData) {
        if (videoStreamData == null) {
            return null;
        }

        synchronized(contactPanelTable) {
            final ContactPanel contactPanel = contactPanelTable.remove(videoStreamData.getStreamName());
            if (contactPanel == null) {
                return null;
            }

            SwingUtilities.invokeLater(() -> {
                contactPanels.remove(contactPanel);
                contactPanels.revalidate();
                contactPanels.repaint();
            });
            return contactPanel;
        }
    }

    /**
     * 処理が完了したかどうかを取得します。
     * @return 処理が完了したかどうか
     */
    public boolean isCompleted() {
        return isCompleted;
    }
}
