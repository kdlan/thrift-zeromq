package com.github.kdlan.thrift.zeromq.service;

import org.apache.thrift.TException;

public class EchoServiceImpl implements EchoService.Iface{

    @Override
    public String echo(String msg) throws TException {
        return msg;
    }

}
