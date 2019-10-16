package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Client bootstrap class for Netty.
 * 
 * @author kedarkhetia
 *
 */
public class Client {

    private Channel channel;
    private EventLoopGroup workerGroup;

    public Client(String hostname, int port, int chunkSize) {
        this.workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(chunkSize);

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);
        ChannelFuture cf = bootstrap.connect(hostname, port);
        cf.syncUninterruptibly();
        this.channel = cf.channel();
    }
   
    /**
     * Method to send messages to Netty Server.
     * @param message
     */
    public void sendMessage(Messages.ProtoMessage message) {
        ChannelFuture write = channel.write(message);
        channel.flush();
        write.syncUninterruptibly();
    }

    /**
     * Method to disconnect client from Netty Server.
     */
    public void disconnect() {
        workerGroup.shutdownGracefully();
    }

}
