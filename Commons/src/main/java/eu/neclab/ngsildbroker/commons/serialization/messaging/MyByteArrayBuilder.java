/*
 * Copyright (c) 1994, 2019, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * This class implements an output stream in which the data is written into a
 * byte array. The buffer automatically grows as data is written to it. The data
 * can be retrieved using {@code toByteArray()} and {@code toString()}.
 * <p>
 * Closing a {@code ByteArrayOutputStream} has no effect. The methods in this
 * class can be called after the stream has been closed without generating an
 * {@code IOException}.
 *
 * @author Arthur van Hoff
 * @since 1.0
 */

public class MyByteArrayBuilder extends OutputStream {

	public static final int SOFT_MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
	/**
	 * The buffer where data is stored.
	 */
	protected byte buf[];
	private List<byte[]> bufs = new ArrayList<>();

	/**
	 * The number of valid bytes in the buffer.
	 */
	protected int count;

	protected int currentCount;
	private int growSize;

	/**
	 * Creates a new {@code ByteArrayOutputStream}. The buffer capacity is initially
	 * 32 bytes, though its size increases if necessary.
	 */
	public MyByteArrayBuilder() {
		this(32);
	}

	/**
	 * Creates a new {@code ByteArrayOutputStream}, with a buffer capacity of the
	 * specified size, in bytes.
	 *
	 * @param size the initial size.
	 * @throws IllegalArgumentException if size is negative.
	 */
	public MyByteArrayBuilder(int size) {
		this.growSize = size / 2;
		if (size < 0) {
			throw new IllegalArgumentException("Negative initial size: " + size);
		}
		buf = new byte[size];
	}

	/**
	 * Increases the capacity if necessary to ensure that it can hold at least the
	 * number of elements specified by the minimum capacity argument.
	 *
	 * @param minCapacity the desired minimum capacity.
	 * @throws OutOfMemoryError if {@code minCapacity < 0} and
	 *                          {@code minCapacity - buf.length > 0}. This is
	 *                          interpreted as a request for the unsatisfiably large
	 *                          capacity.
	 *                          {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
	 */

	/**
	 * Writes the specified byte to this {@code ByteArrayOutputStream}.
	 *
	 * @param b the byte to be written.
	 */
	public synchronized void write(int b) {
		if (currentCount >= buf.length) {
			bufs.add(buf);
			buf = new byte[growSize];
			currentCount = 0;
		}
		buf[currentCount++] = (byte) b;
		count += 1;

	}

	/**
	 * Writes {@code len} bytes from the specified byte array starting at offset
	 * {@code off} to this {@code ByteArrayOutputStream}.
	 *
	 * @param b   the data.
	 * @param off the start offset in the data.
	 * @param len the number of bytes to write.
	 * @throws NullPointerException      if {@code b} is {@code null}.
	 * @throws IndexOutOfBoundsException if {@code off} is negative, {@code len} is
	 *                                   negative, or {@code len} is greater than
	 *                                   {@code b.length - off}
	 */
	public synchronized void write(byte b[], int off, int len) {
		// Objects.checkFromIndexSize(off, len, b.length);
		while (true) {
			int max = buf.length - currentCount;
			int toCopy = Math.min(max, len);
			if (toCopy > 0) {
				System.arraycopy(b, off, buf, currentCount, toCopy);
				off += toCopy;
				currentCount += toCopy;
				count += toCopy;
				len -= toCopy;
			}
			if (len <= 0)
				break;
			bufs.add(buf);
			buf = new byte[growSize];
			currentCount = 0;
		}
	}

	public synchronized byte[] finalizeMessage(int baseRollbackpoint) {
		byte[] result = new byte[count + 1];
		int offset = 0;
		for (int i = 0; i < bufs.size(); i++) {
			byte[] b = bufs.get(i);
			System.arraycopy(b, 0, result, offset, b.length);
			offset += b.length;
		}

		System.arraycopy(buf, 0, result, offset, currentCount + 1);
		offset += currentCount;
		result[offset - 1] = ']';
		result[offset] = '}';
		if (!bufs.isEmpty()) {
			buf = bufs.get(0);
			bufs.clear();
		}
		currentCount = count = baseRollbackpoint;
		return result;
	}

	public synchronized byte[] rollback(int rollbackPoint, int baseRollbackpoint) {
		int targetCount = count - rollbackPoint;
		if (targetCount < 0) {
			throw new IllegalArgumentException("Rollback point exceeds the total count");
		}

		int bytesToRemove = count - targetCount;
		byte[] removedArray = new byte[bytesToRemove];
		int removedIndex = bytesToRemove;

		// Remove bytes from the current buffer
		int bytesFromCurrentBuf = Math.min(bytesToRemove, currentCount);
		removedIndex -= bytesFromCurrentBuf;
		System.arraycopy(buf, currentCount - bytesFromCurrentBuf, removedArray, removedIndex, bytesFromCurrentBuf);
		count -= bytesFromCurrentBuf;

		// Remove bytes from previous buffers if necessary
		while (count > targetCount) {
			buf = bufs.remove(bufs.size() - 1);
			int bytesFromBuf = Math.min(buf.length, count - targetCount);
			removedIndex -= bytesFromBuf;
			System.arraycopy(buf, buf.length - bytesFromBuf, removedArray, removedIndex, bytesFromBuf);
			count -= bytesFromBuf;
		}
		byte[] resultBytes = finalizeMessage(baseRollbackpoint);
		write(removedArray);
		return resultBytes;
	}

	/**
	 * Writes the complete contents of the specified byte array to this
	 * {@code ByteArrayOutputStream}.
	 *
	 * @apiNote This method is equivalent to {@link #write(byte[],int,int) write(b,
	 *          0, b.length)}.
	 *
	 * @param b the data.
	 * @throws NullPointerException if {@code b} is {@code null}.
	 * @since 11
	 */
	public void write(byte b[]) {
		write(b, 0, b.length);
	}

	/**
	 * Writes the complete contents of this {@code ByteArrayOutputStream} to the
	 * specified output stream argument, as if by calling the output stream's write
	 * method using {@code out.write(buf, 0, count)}.
	 *
	 * @param out the output stream to which to write the data.
	 * @throws NullPointerException if {@code out} is {@code null}.
	 * @throws IOException          if an I/O error occurs.
	 */
	public synchronized void writeTo(OutputStream out) throws IOException {
		out.write(toByteArray(), 0, count);
	}

	/**
	 * Resets the {@code count} field of this {@code ByteArrayOutputStream} to zero,
	 * so that all currently accumulated output in the output stream is discarded.
	 * The output stream can be used again, reusing the already allocated buffer
	 * space.
	 *
	 * @see java.io.ByteArrayInputStream#count
	 */
	public synchronized void reset() {
		count = 0;
		currentCount = 0;
		if (!bufs.isEmpty()) {
			buf = bufs.get(0);
			bufs.clear();
		}
	}

	/**
	 * Creates a newly allocated byte array. Its size is the current size of this
	 * output stream and the valid contents of the buffer have been copied into it.
	 *
	 * @return the current contents of this output stream, as a byte array.
	 * @see java.io.ByteArrayOutputStream#size()
	 */
	public synchronized byte[] toByteArray() {
		byte[] result = new byte[count];
		int offset = 0;
		for (int i = 0; i < bufs.size(); i++) {
			byte[] block = bufs.get(i);
			int len = block.length;
			System.arraycopy(block, 0, result, offset, len);
			offset += len;
		}
		System.arraycopy(buf, 0, result, offset, currentCount);
		return result;

	}

	/**
	 * Returns the current size of the buffer.
	 *
	 * @return the value of the {@code count} field, which is the number of valid
	 *         bytes in this output stream.
	 * @see java.io.ByteArrayOutputStream#count
	 */
	public synchronized int size() {
		return count;
	}

	/**
	 * Converts the buffer's contents into a string decoding bytes using the
	 * platform's default character set. The length of the new {@code String} is a
	 * function of the character set, and hence may not be equal to the size of the
	 * buffer.
	 *
	 * <p>
	 * This method always replaces malformed-input and unmappable-character
	 * sequences with the default replacement string for the platform's default
	 * character set. The {@linkplain java.nio.charset.CharsetDecoder} class should
	 * be used when more control over the decoding process is required.
	 *
	 * @return String decoded from the buffer's contents.
	 * @since 1.1
	 */
	public synchronized String toString() {

		return new String(toByteArray());
	}

	/**
	 * Converts the buffer's contents into a string by decoding the bytes using the
	 * named {@link java.nio.charset.Charset charset}.
	 *
	 * <p>
	 * This method is equivalent to {@code #toString(charset)} that takes a
	 * {@link java.nio.charset.Charset charset}.
	 *
	 * <p>
	 * An invocation of this method of the form
	 *
	 * <pre> {@code
	 *      ByteArrayOutputStream b = ...
	 *      b.toString("UTF-8")
	 *      }
	 * </pre>
	 *
	 * behaves in exactly the same way as the expression
	 *
	 * <pre> {@code
	 *      ByteArrayOutputStream b = ...
	 *      b.toString(StandardCharsets.UTF_8)
	 *      }
	 * </pre>
	 *
	 *
	 * @param charsetName the name of a supported {@link java.nio.charset.Charset
	 *                    charset}
	 * @return String decoded from the buffer's contents.
	 * @throws UnsupportedEncodingException If the named charset is not supported
	 * @since 1.1
	 */
	public synchronized String toString(String charsetName) throws UnsupportedEncodingException {
		return new String(toByteArray(), charsetName);
	}

	/**
	 * Converts the buffer's contents into a string by decoding the bytes using the
	 * specified {@link java.nio.charset.Charset charset}. The length of the new
	 * {@code String} is a function of the charset, and hence may not be equal to
	 * the length of the byte array.
	 *
	 * <p>
	 * This method always replaces malformed-input and unmappable-character
	 * sequences with the charset's default replacement string. The
	 * {@link java.nio.charset.CharsetDecoder} class should be used when more
	 * control over the decoding process is required.
	 *
	 * @param charset the {@linkplain java.nio.charset.Charset charset} to be used
	 *                to decode the {@code bytes}
	 * @return String decoded from the buffer's contents.
	 * @since 10
	 */
	public synchronized String toString(Charset charset) {
		return new String(toByteArray(), charset);
	}

	/**
	 * Creates a newly allocated string. Its size is the current size of the output
	 * stream and the valid contents of the buffer have been copied into it. Each
	 * character <i>c</i> in the resulting string is constructed from the
	 * corresponding element <i>b</i> in the byte array such that: <blockquote>
	 * 
	 * <pre>{@code
	 * c == (char) (((hibyte & 0xff) << 8) | (b & 0xff))
	 * }</pre>
	 * 
	 * </blockquote>
	 *
	 * @deprecated This method does not properly convert bytes into characters. As
	 *             of JDK&nbsp;1.1, the preferred way to do this is via the
	 *             {@link #toString(String charsetName)} or
	 *             {@link #toString(Charset charset)} method, which takes an
	 *             encoding-name or charset argument, or the {@code toString()}
	 *             method, which uses the platform's default character encoding.
	 *
	 * @param hibyte the high byte of each resulting Unicode character.
	 * @return the current contents of the output stream, as a string.
	 * @see java.io.ByteArrayOutputStream#size()
	 * @see java.io.ByteArrayOutputStream#toString(String)
	 * @see java.io.ByteArrayOutputStream#toString()
	 */
	@Deprecated
	public synchronized String toString(int hibyte) {
		return new String(toByteArray(), hibyte, 0, count);
	}

	/**
	 * Closing a {@code ByteArrayOutputStream} has no effect. The methods in this
	 * class can be called after the stream has been closed without generating an
	 * {@code IOException}.
	 */
	public void close() throws IOException {
	}

	public void reduceByOne() {
		count--;
		currentCount--;

	}

}
