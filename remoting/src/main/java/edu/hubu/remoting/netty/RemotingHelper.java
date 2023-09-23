package edu.hubu.remoting.netty;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author: sugar
 * @date: 2023/5/22
 * @description:
 */
public class RemotingHelper {

    public static String parseChannel2RemoteAddress(Channel channel){
        if(channel == null) return "";
        SocketAddress socketAddress = channel.remoteAddress();
        String address =  socketAddress != null ? socketAddress.toString(): "";
        if(address.length() > 0){
            int index = address.lastIndexOf("/");
            if(index >= 0){
                return address.substring(index + 1);
            }
        }
        return "";
    }

    public static SocketAddress string2SocketAddress(String address){
        int index = address.lastIndexOf(":");
        String host = address.substring(0, index);
        String port = address.substring(index + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }
}
