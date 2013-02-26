package com.github.kdlan.thrift.zeromq;

import static org.junit.Assert.assertEquals;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.jeromq.ZMQ;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.kdlan.thrift.zeromq.service.EchoService;
import com.github.kdlan.thrift.zeromq.service.EchoServiceImpl;

public class TestThriftOverZeroMQ {
    Logger logger =LoggerFactory.getLogger(this.getClass());
    @Test
    public void test() throws TException{
        ZMQ.Context context=ZMQ.context();
        TZeroMQSimpleServer.Args args=new TZeroMQSimpleServer.Args(context, "inproc://thrift_test");
        EchoService.Processor<EchoService.Iface> process = new EchoService.Processor<EchoService.Iface>(
                new EchoServiceImpl());
        args.processor(process);
        final TZeroMQSimpleServer server=new TZeroMQSimpleServer(args);
        Thread t=new Thread(){
            public void run() {
                server.serve();
            };
        };
       t.start();

       TTransport transport=new TZeroMQTransport(context, "inproc://thrift_test", ZMQ.REQ, false);
       transport.open();
       TProtocol protocol = new TBinaryProtocol(transport);
       EchoService.Client client=new EchoService.Client(protocol);

       String msg="tsae;jlkjfa";
       assertEquals(msg,client.echo("tsae;jlkjfa"));

       transport.close();
       server.stop();
       context.term();

    }
}
