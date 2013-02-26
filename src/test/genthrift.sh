rm -rf gen-javabean
rm -rf java/com/github/kdlan/thrift/zeromq/service

thrift --gen java:beans,hashcode test.thrift

mv gen-javabean/com/github/kdlan/thrift/zeromq/service java/com/github/kdlan/thrift/zeromq/service
rm -rf gen-javabean
