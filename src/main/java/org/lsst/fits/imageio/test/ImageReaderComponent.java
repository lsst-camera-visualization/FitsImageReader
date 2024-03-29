package org.lsst.fits.imageio.test;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.swing.JComponent;
import javax.swing.JScrollPane;

/**
 * Simple component for displaying a buffered image
 *
 * @author tonyj
 */
public final class ImageReaderComponent extends JComponent {

    private static final long serialVersionUID = 1L;
    private float scale = 1;
    private ImageReader reader;
    private float minScale = 0;
    private final float maxScale = 10;
    private JScrollPane scroll;
    private boolean zoomScaleSet = false;
    private int imageHeight;
    private int imageWidth;
    private BufferedImage bi;
    private ImageReadParam param;

    public ImageReaderComponent() {
        this(false);
    }

    public ImageReaderComponent(boolean zoomPan) {
        reader = null;
        this.setLayout(new BorderLayout());
        Inner inner = new Inner();
        if (zoomPan) {
            scroll = new JScrollPane(inner);
            add(scroll);
        } else {
            add(inner);
        }
        addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                if (zoomScaleSet) {
                    minScale = Math.min((float) getHeight() / imageHeight, (float) getWidth() / imageWidth);
                    minScale = Math.min(minScale, maxScale);
                    scale = Math.max(scale, minScale);
                }
            }
        });
    }

    public ImageReaderComponent(boolean zoomPan, ImageReader image, ImageReadParam param) throws IOException {
        this(zoomPan);
        setImageReader(image, param);
    }

    @Override
    public Dimension getPreferredSize() {
        Dimension size = new Dimension(200, 200);
        if (reader != null) {
            size.width = imageWidth;
            size.height = imageHeight;
        }
        return size;
    }

    final void setImageReader(ImageReader image, ImageReadParam param) throws IOException {
        this.reader = image;
        this.param = param;
        this.imageHeight = image.getHeight(0);
        this.imageWidth = image.getWidth(0);
        this.zoomScaleSet = false;
        repaint();
    }

    private void checkScale() {
        if (!zoomScaleSet) {
            minScale = Math.min((float) getHeight() / imageHeight, (float) getWidth() / imageWidth);
            minScale = Math.min(minScale, maxScale);
            scale = minScale;
            zoomScaleSet = true;
        }
    }

    ImageReader getImage() {
        return reader;
    }

    Point getImagePosition(Point point) {
        Rectangle visibleRect = scroll.getViewport().getVisibleRect();
        return new Point(Math.round(visibleRect.x + point.x / scale), Math.round(imageHeight - visibleRect.y - point.y / scale));
    }

    private final class Inner extends JComponent {

        private static final long serialVersionUID = 1L;

        Inner() {
            MouseAdapter a = new MouseAdapter() {
                private Point savePoint;
                private Rectangle saveViewRectangle;

                @Override
                public void mouseDragged(MouseEvent e) {
                    Rectangle newRectangle = new Rectangle(saveViewRectangle);
                    newRectangle.x += savePoint.x - e.getX();
                    newRectangle.y += savePoint.y - e.getY();
                    scroll.getViewport().scrollRectToVisible(newRectangle);
                }

                @Override
                public void mousePressed(MouseEvent e) {
                    savePoint = e.getPoint();
                    saveViewRectangle = scroll.getViewport().getVisibleRect();
                }

                @Override
                public void mouseWheelMoved(MouseWheelEvent e) {
                    if (e.getPreciseWheelRotation() > 0) {
                        zoom(e.getPoint(), 1 / 0.9f);
                    } else {
                        zoom(e.getPoint(), 0.9f);
                    }
                }

                @Override
                public void mouseMoved(MouseEvent e) {
                    ImageReaderComponent.this.dispatchEvent(e);
                }
            };

            addMouseWheelListener(a);
            addMouseMotionListener(a);
            addMouseListener(a);
        }

        public void zoom(Point point, float zoomFactor) {
            Point pos = scroll.getViewport().getViewPosition();
            Point.Float mouseRelativeToImage = new Point.Float((point.x) / scale, (point.y) / scale);
            scale *= zoomFactor;
            scale = Math.max(minScale, Math.min(maxScale, scale));
            Point.Float mouseRelativeToImageNew = new Point.Float(point.x / scale, point.y / scale);
            Point.Float shiftNeeded = new Point.Float(mouseRelativeToImageNew.x - mouseRelativeToImage.x,
                    mouseRelativeToImageNew.y - mouseRelativeToImage.y);
            float newX = pos.x - shiftNeeded.x * scale;
            float newY = pos.y - shiftNeeded.y * scale;
            final Point newPos = new Point(Math.round(newX), Math.round(newY));
            scroll.getViewport().setViewPosition(newPos);
            revalidate();
            repaint();
        }

        @Override
        public Dimension getPreferredSize() {
            Dimension size = new Dimension(200, 200);
            if (reader != null) {
                checkScale();
                size.width = (int) (imageWidth * scale);
                size.height = (int) (imageWidth * scale);
            }
            return size;
        }

        @Override
        protected void paintComponent(Graphics g) {
            if (reader != null) {
                Graphics2D g2 = (Graphics2D) g;
                AffineTransform at = new AffineTransform();
                at.scale(1, -1);
                at.translate(0, -getHeight());
                at.scale(scale, scale);
                //((Graphics2D) g).drawImage(image, at, this);
                Rectangle sourceRegion = scroll.getViewport().getViewRect();
                if (scale != 1.0) {
                    double factor = 1.0 / scale;
                    sourceRegion.x *= factor;
                    sourceRegion.y *= factor;
                    sourceRegion.width *= factor;
                    sourceRegion.height *= factor;
                }
                Rectangle viewRect = scroll.getViewport().getViewRect();
                //System.out.printf("Size: %d %d %s\n", imageWidth, imageHeight, viewRect);
                int subSamplingX = (int) Math.max(1, Math.floor(sourceRegion.width / viewRect.getWidth()));
                int subSamplingY = (int) Math.max(1, Math.floor(sourceRegion.height / viewRect.getHeight()));
                //System.out.printf("%s %s %s %d %d\n", g2.getClipBounds(), scale, sourceRegion, subSamplingX, subSamplingY);

                param.setSourceRegion(sourceRegion);
                param.setSourceSubsampling(subSamplingX, subSamplingY, 0, 0);
                try {
                    bi = reader.read(0, param);
                    //System.out.printf("BufferedImage: %d %d\n", bi.getWidth(), bi.getHeight());
                    g2.drawImage(bi, viewRect.x, viewRect.y, viewRect.width, viewRect.height, this);
                } catch (IOException ex) {
                    Logger.getLogger(ImageReaderComponent.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
}
