package edu.usfca.cs.dfs.storage.client;

import edu.usfca.cs.dfs.controller.net.ControllerInboundHandler;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ContorllerClient {

    private Channel channel;
    private EventLoopGroup workerGroup;

    public ContorllerClient(String hostname, int port) {
        ControllerInboundHandler controllerInboundHandler = new ControllerInboundHandler();
        this.workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(controllerInboundHandler);

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);
        ChannelFuture cf = bootstrap.connect(hostname, port);
        cf.syncUninterruptibly();
        this.channel = cf.channel();
    }


    public void sendMessage(Messages.ProtoMessage message) {
        ChannelFuture write = channel.write(message);
        channel.flush();
        write.syncUninterruptibly();
    }

    public void disconnect() {
        workerGroup.shutdownGracefully();
    }

}
