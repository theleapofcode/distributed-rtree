package com.theleapofcode.hazelcast.rtree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.github.davidmoten.rtree.InternalStructure;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.Serializer;
import com.github.davidmoten.rtree.Serializers;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

public class RTreeSerializer implements StreamSerializer<RTree<String, Geometry>> {

	private Serializer<String, Geometry> serializer = Serializers.flatBuffers().utf8();

	public RTreeSerializer() {
		super();
	}

	public int getTypeId() {
		return 2;
	}

	public void write(ObjectDataOutput objectDataOutput, RTree<String, Geometry> rTree) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.write(rTree, baos);

		byte[] bytes = baos.toByteArray();
		objectDataOutput.write(bytes);
	}

	public RTree<String, Geometry> read(ObjectDataInput objectDataInput) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = ((InputStream) objectDataInput).read(data, 0, data.length)) != -1) {
			baos.write(data, 0, nRead);
		}

		baos.flush();

		byte[] bytes = baos.toByteArray();

		ByteArrayInputStream input = new ByteArrayInputStream(bytes);
		return serializer.read(input, bytes.length, InternalStructure.DEFAULT);
	}

	public void destroy() {
	}
}