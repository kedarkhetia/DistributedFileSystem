package edu.usfca.cs.dfs.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
extends SimpleChannelInboundHandler<Messages.ProtoMessage> {

    public InboundHandler() { }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        //System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        //System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.ProtoMessage msg) 
    		throws InvalidMessageException, IOException, InterruptedException, ExecutionException, NoSuchAlgorithmException {
        if(msg.hasController()) {
            Messages.ProtoMessage resp = edu.usfca.cs.dfs.controller.MessageDispatcher.dispatch(msg.getController());
            if(resp != null) {
                ctx.writeAndFlush(resp);
            }
        }
        else if(msg.hasStorage()) {
            Messages.ProtoMessage resp = edu.usfca.cs.dfs.storage.MessageDispatcher.dispatch(msg.getStorage());
            if(resp != null) {
                ctx.writeAndFlush(resp);
            }
        }
        else if(msg.hasClient()) {
            Messages.ProtoMessage resp = edu.usfca.cs.dfs.dfsclient.MessageDispatcher.dispatch(msg.getClient());
            if(resp != null) {
                ctx.writeAndFlush(resp);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
