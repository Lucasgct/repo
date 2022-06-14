/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.apache.lucene.codecs.experimental;

import java.io.IOException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** LZ4 JNI based Compression Mode */
public class LZ4CompressionMode extends CompressionMode {
    private static final int NUM_SUB_BLOCKS = 10;

    /** default constructor */
    public LZ4CompressionMode() {}

    @Override
    public Compressor newCompressor() {
        return new LZ4CompressionMode.LZ4InnerCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
        return new LZ4CompressionMode.LZ4InnerDecompressor();
    }

    /** LZ4 compressor */
    private static final class LZ4InnerCompressor extends Compressor {
        byte[] compressedBuffer;
        LZ4Compressor compressor;

        /** Default constructor */
        public LZ4InnerCompressor() {
            compressedBuffer = BytesRef.EMPTY_BYTES;
            compressor = LZ4Factory.nativeInstance().fastCompressor();
        }

        @Override
        public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
            int blockLength = (len + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLength);

            final int end = off + len;

            for (int start = off; start < end; start += blockLength) {
                int l = Math.min(blockLength, off + len - start);

                if (l == 0) {
                    out.writeVInt(0);
                    return;
                }

                final int maxCompressedLength = compressor.maxCompressedLength(l);
                compressedBuffer = ArrayUtil.grow(compressedBuffer, maxCompressedLength);

                int compressedSize = compressor.compress(bytes, start, l, compressedBuffer, 0, compressedBuffer.length);

                out.writeVInt(compressedSize);
                out.writeBytes(compressedBuffer, compressedSize);
            }
        }

        @Override
        public void close() throws IOException {}
    }

    // decompression

    /** LZ4 decompressor */
    private static final class LZ4InnerDecompressor extends Decompressor {

        byte[] compressed;
        LZ4FastDecompressor decompressor;

        /** default decompressor */
        public LZ4InnerDecompressor() {
            compressed = BytesRef.EMPTY_BYTES;
            decompressor = LZ4Factory.nativeInstance().fastDecompressor();
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength;

            if (length == 0) {
                bytes.length = 0;
                return;
            }

            final int blockLength = in.readVInt();
            bytes.offset = bytes.length = 0;
            int offsetInBlock = 0;
            int offsetInBytesRef = offset;

            // Skip unneeded blocks
            while (offsetInBlock + blockLength < offset) {
                final int compressedLength = in.readVInt();
                in.skipBytes(compressedLength);
                offsetInBlock += blockLength;
                offsetInBytesRef -= blockLength;
            }

            // Read blocks that intersect with the interval we need
            while (offsetInBlock < offset + length) {
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + blockLength);
                final int compressedLength = in.readVInt();
                if (compressedLength == 0) {
                    return;
                }
                compressed = ArrayUtil.grow(compressed, compressedLength);
                in.readBytes(compressed, 0, compressedLength);

                int l = Math.min(blockLength, originalLength - offsetInBlock);
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + l);

                byte[] output = new byte[l];

                decompressor.decompress(compressed, 0, output, 0, l);
                System.arraycopy(output, 0, bytes.bytes, bytes.length, l);

                bytes.length += l;
                offsetInBlock += blockLength;
            }

            bytes.offset = offsetInBytesRef;
            bytes.length = length;
            assert bytes.isValid();
        }

        @Override
        public Decompressor clone() {
            return new LZ4InnerDecompressor();
        }
    }
}
