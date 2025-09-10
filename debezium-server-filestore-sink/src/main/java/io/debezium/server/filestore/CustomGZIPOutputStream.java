/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.filestore;

import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.io.OutputStream;
import java.util.zip.CRC32;

/**
 * Custom implementation of GZIPOutputStream that allows reuse of Deflater
 * to avoid allocation overhead.
 */
public class CustomGZIPOutputStream extends DeflaterOutputStream {
    private static final int GZIP_MAGIC = 0x8b1f;
    private final CRC32 crc = new CRC32();
    private boolean headerWritten = false;
    private final byte[] header = new byte[10];
    protected byte[] buf = new byte[512];

    public CustomGZIPOutputStream(OutputStream out, Deflater deflater) throws IOException {
        super(out, deflater, 512, false); // Don't sync flush
        this.def = deflater;
    }

    @Override 
    public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte)(b & 0xff);
        write(buf, 0, 1);
    }

    @Override 
    public void write(byte[] buf, int off, int len) throws IOException {
        if (!headerWritten) {
            writeHeader();
            headerWritten = true;
        }
        if (len > 0) {
            crc.update(buf, off, len);
            super.write(buf, off, len);
        }
    }

    private void writeHeader() throws IOException {
        header[0] = (byte) GZIP_MAGIC;
        header[1] = (byte) (GZIP_MAGIC >> 8);
        header[2] = Deflater.DEFLATED;
        header[3] = 0;
        header[4] = 0;
        header[5] = 0;
        header[6] = 0;
        header[7] = 0;
        header[8] = 0;
        header[9] = 0;
        out.write(header, 0, header.length);
    }

    @Override 
    public void finish() throws IOException {
        if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
                int len = def.deflate(buf, 0, buf.length);
                if (def.finished() && len <= buf.length - 8) {
                    writeTrailer(buf, len);
                    len += 8;
                    out.write(buf, 0, len);
                    return;
                }
                if (len > 0) {
                    out.write(buf, 0, len);
                }
            }
            byte[] trailer = new byte[8];
            writeTrailer(trailer, 0);
            out.write(trailer);
        }
    }

    private void writeTrailer(byte[] trailer, int offset) {
        writeInt((int) crc.getValue(), trailer, offset);
        writeInt(def.getTotalIn(), trailer, offset + 4);
    }

    private static void writeInt(int i, byte[] buf, int offset) {
        writeShort(i & 0xffff, buf, offset);
        writeShort((i >> 16) & 0xffff, buf, offset + 2);
    }

    private static void writeShort(int s, byte[] buf, int offset) {
        buf[offset] = (byte)(s & 0xff);
        buf[offset + 1] = (byte)((s >> 8) & 0xff);
    }
}
