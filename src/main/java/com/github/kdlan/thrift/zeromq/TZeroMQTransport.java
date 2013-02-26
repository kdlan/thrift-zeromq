package com.github.kdlan.thrift.zeromq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jeromq.ZMQ;

public class TZeroMQTransport extends TTransport {

    private ZMQ.Socket socket;
    private final ZMQ.Context context;
    private final List<String> endpoints;
    private final int type;
    private final boolean bind;

    private final TMemoryInputTransport is = new TMemoryInputTransport();
    private final ByteArrayOutputStream os=new ByteArrayOutputStream(1024);

    public TZeroMQTransport(ZMQ.Context context, List<String> endpoints,
            int type, boolean bind) {
        super();
        this.context = context;
        this.endpoints = endpoints;
        this.type = type;
        this.bind = bind;
    }

    public TZeroMQTransport(ZMQ.Context context, String endpoint, int type,
            boolean bind) {
        this(context, Arrays.asList(endpoint), type, bind);
    }

    @Override
    public boolean isOpen() {
        return socket != null;
    }

    @Override
    public void open() throws TTransportException {
        if (socket == null) {
            socket = context.socket(type);
            socket.setLinger(0);
            for (String endpoint : endpoints) {
                if (bind) {
                    socket.bind(endpoint);
                } else {
                    socket.connect(endpoint);
                }
            }
        }
    }

    @Override
    public void close() {
        socket.close();
        socket = null;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (isOpen()) {
            checkRead();
            return is.read(buf, off, len);
        } else {
            throw new IllegalStateException("transport not open");
        }
    }

    private void checkRead() {
        // TODO process zmq envelop
        if (is.getBuffer() == null||is.getBytesRemainingInBuffer()==0) {
            byte[] data = socket.recv();
            is.reset(data);
        }
    }




    @Override
    public void flush() throws TTransportException {
        byte[] data=os.toByteArray();
        os.reset();
        //TODO send zmq envelop;
        socket.send(data);
    }


    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        os.write(buf, off, len);
    }

}
