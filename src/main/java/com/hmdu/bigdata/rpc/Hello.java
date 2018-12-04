package com.hmdu.bigdata.rpc;

// 这里好像是可以实现一个接口：VersionedProtocol
// 然后可以在这里用定义的versionID来实现getProtocolVersion方法
// 当然也可以在实现Hello接口的类中按照同样的方式实现这个方法
// 这里要说明，如果是在Hello的实现类中（客户端类和服务器类中）实现getProtocolVersion方法，
// 那么客户端类和服务器类中的版本号要一致，这就是版本控制的作用
// 这个接口的作用就是保证客户端和服务器端的通信端口一致
public interface Hello {

    long versionID = 1L;

    String say(String words);
}
