package com.github.kdlan.thrift.zeromq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;

public class TZeroMQSimpleServer {

    public static class Args {
        final ZMQ.Context context;
        final String endpoint;
        TProcessorFactory processorFactory;
        TProtocolFactory inputProtocolFactory = new TBinaryProtocol.Factory();
        TProtocolFactory outputProtocolFactory = new TBinaryProtocol.Factory();

        public Args(Context context, String endpoint) {
            this.context = context;
            this.endpoint = endpoint;
        }

        public Args processorFactory(TProcessorFactory factory) {
            this.processorFactory = factory;
            return this;
        }

        public Args processor(TProcessor processor) {
            this.processorFactory = new TProcessorFactory(processor);
            return this;
        }

        public Args protocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            this.outputProtocolFactory = factory;
            return this;
        }

        public Args inputProtocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            return this;
        }

        public Args outputProtocolFactory(TProtocolFactory factory) {
            this.outputProtocolFactory = factory;
            return this;
        }
    }

    private final ZMQ.Socket socket;
    private volatile boolean running = false;
    private final TProcessorFactory processorFactory;
    private final TProtocolFactory inputProtocolFactory;
    private final TProtocolFactory outputProtocolFactory;
    private final CountDownLatch stopLatch=new CountDownLatch(1);

    public TZeroMQSimpleServer(Args args) {
        socket = args.context.socket(ZMQ.REP);
        socket.setLinger(0);
        socket.setReceiveTimeOut(1000);
        socket.bind(args.endpoint);
        processorFactory=args.processorFactory;
        inputProtocolFactory=args.inputProtocolFactory;
        outputProtocolFactory=args.outputProtocolFactory;
    }

    public void serve() {
        running=true;
        while(running){
            byte[] data=socket.recv();
            if(data==null){
                continue;
            }

            ByteArrayInputStream input=new ByteArrayInputStream(data);
            ByteArrayOutputStream output=new ByteArrayOutputStream(256);
            TTransport transport=new TIOStreamTransport(input, output);
            TProcessor processor=processorFactory.getProcessor(transport);
            TProtocol inpro=inputProtocolFactory.getProtocol(transport);
            TProtocol outpro=outputProtocolFactory.getProtocol(transport);
            try {
                processor.process(inpro, outpro);
            } catch (TException e) {
                e.printStackTrace();
            }
            byte[] result=output.toByteArray();
            socket.send(result);
        }
        stopLatch.countDown();

    }

    public void stop(){
        running=false;
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
        }
        socket.close();
    }

}
