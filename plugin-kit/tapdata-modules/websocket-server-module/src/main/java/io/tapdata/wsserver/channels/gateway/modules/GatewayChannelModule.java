package io.tapdata.wsserver.channels.gateway.modules;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.Identity;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.data.ResultData;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.NodeRegistryService;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.JWTUtils;
import io.tapdata.wsserver.channels.error.WSErrors;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.gateway.data.UserChannel;
import io.tapdata.wsserver.channels.websocket.event.ChannelInActiveEvent;
import io.tapdata.wsserver.channels.websocket.event.IdentityReceivedEvent;
import io.tapdata.wsserver.channels.websocket.impl.WebSocketProperties;
import io.tapdata.wsserver.channels.websocket.utils.NetUtils;
import io.tapdata.wsserver.eventbus.EventBusHolder;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Bean()
@MainMethod(value = "main", order = 10000)
public class GatewayChannelModule {
    private static final String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";

    public void main() {
        EventBusHolder.getEventBus().register(this);
    }

    public static final String KEY_GATEWAY_USER = "gwuser";
    public static final String TAG = GatewayChannelModule.class.getSimpleName();
    @Bean
    private WebSocketProperties webSocketProperties;
    @Bean
    private GatewaySessionManager gatewaySessionManager;
    
    private long userSessionCacheExpireSeconds = 1800;
    @Bean
    private JsonParser jsonParser;

    @Bean
    private NodeRegistryService nodeRegistryService;

    private final ConcurrentHashMap<String, ChannelHandlerContext> userIdChannelMap = new ConcurrentHashMap<>();
    
    public boolean close(String userId, Integer code) {
        ChannelHandlerContext context = userIdChannelMap.remove(userId);
        if (context != null) {
            Channel channel = context.channel();
            if (channel != null && channel.isActive()) {
                Result result = new Result().code(code);
                NetUtils.writeAndFlush(channel, result);
                channel.close();
                return true;
            }
        }
        return false;
    }

    public boolean sendResultData(String userId, ResultData resultData) {
        if (resultData == null || userId == null) {
            TapLogger.error(TAG, "sendResultData ignored, because illegal arguments, userId $userId, resultData $resultData");
            return false;
        }
        Result result = new Result().code(resultData.getCode()).message(resultData.getMessage()).contentType(resultData.getContentType()).forId(resultData.getForId()).time(System.currentTimeMillis()).contentEncode(resultData.getContentEncode());

        return sendData(userId, result);
    }

    public boolean sendData(String userId, Data data) {
        ChannelHandlerContext context = userIdChannelMap.get(userId);
        if (context != null) {
            Channel channel = context.channel();
            if (channel != null) {
                return NetUtils.writeAndFlush(channel, data);
            }
        }
        return false;
    }

    @Subscribe
    @AllowConcurrentEvents
    public void receivedIdentity(IdentityReceivedEvent identityReceivedEvent) {
        Identity identity = identityReceivedEvent.getIdentity();
        if (identity == null || identity.getToken() == null) {
            identityReceivedEvent.closeChannel(identity.getId(), WSErrors.ERROR_ILLEGAL_TOKEN);
            return;
        }
        //TODO identity数据需要实现验证签名逻辑， 保护数据不被修改
//        if(identity.sign == null || sign(identity.sign)) {
//
//        }
        CommonUtils.handleAnyError(() -> {
            Map<String, Object> claims = JWTUtils.getClaims(key, identity.getToken());
            String service = (String) claims.get("service");
            String clientId = (String) claims.get("clientId");
            Integer terminal = (Integer) claims.get("terminal");
            String uid = (String) claims.get("uid");

            GatewaySessionHandler gatewaySessionHandler = gatewaySessionManager.preCreateGatewaySessionHandler(clientId, service, null, clientId, terminal, true, null);

            ChannelHandlerContext old = userIdChannelMap.put(clientId, identityReceivedEvent.getCtx());
            if (old != null) {
                identityReceivedEvent.closeChannel(old.channel(), identity.getId(), WSErrors.ERROR_CHANNEL_KICKED);
//            return
            }

            Channel channel = identityReceivedEvent.getCtx().channel();

            InetSocketAddress insocket = (InetSocketAddress) channel.remoteAddress();
            String clientIP = insocket.getAddress().getHostAddress();
            gatewaySessionHandler.getUserChannel().setIp(clientIP);

            Attribute<UserChannel> attribute = channel.attr(AttributeKey.valueOf(KEY_GATEWAY_USER));
            attribute.set(gatewaySessionHandler.getUserChannel());

            int httpPort = 3000; //TODO should read from TM config.
            nodeRegistryService.save(new NodeRegistry().ip(clientIP).httpPort(httpPort).wsPort(webSocketProperties.getPort()).type(service).time(System.currentTimeMillis()));

            //OnConnected//异步
            //sendCache//同步
            TapEntity message = gatewaySessionManager.channelConnected(gatewaySessionHandler);

            identityReceivedEvent.sendResult(new Result().forId(identity.getId()).code(Data.CODE_SUCCESS).message(message).time(System.currentTimeMillis()));
        }, throwable -> {
            int code = WSErrors.ERROR_UNKNOWN;
            if(throwable instanceof CoreException) {
                CoreException coreException = (CoreException) throwable;
                code = coreException.getCode();
            }
            identityReceivedEvent.closeChannel(identity.getId(), code, throwable.getMessage());
        });

    }

    @Subscribe
    @AllowConcurrentEvents
    public void receivedChannelInactive(ChannelInActiveEvent channelInActiveEvent) {
        Channel channel = channelInActiveEvent.getCtx().channel();
        Attribute<UserChannel> attribute = channel.attr(AttributeKey.valueOf(KEY_GATEWAY_USER));
        UserChannel userChannel = attribute.get();

        GatewaySessionHandler gatewaySessionHandler = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().get(userChannel.getUserId());
        if(gatewaySessionHandler != null)
            gatewaySessionManager.channelDisconnected(gatewaySessionHandler);

        final UserChannel userSession = attribute.getAndSet(null);
        if (userSession != null) {
            boolean bool = userIdChannelMap.remove(userSession.getUserId(), channelInActiveEvent.getCtx());
            if (!bool) {
                TapLogger.warn(TAG, "userIdChannelMap remove userId {} failed, because channel not the same, closed channel {} not removed channel {}", userSession.getUserId(), channelInActiveEvent.getCtx(), userIdChannelMap.get(userSession.getUserId()));
            }
        }
    }

    public boolean isChannelActive(String userId) {
        ChannelHandlerContext context = userIdChannelMap.get(userId);
        if (context != null) {
            Channel channel = context.channel();
            if (channel != null) {
                return true;
            }
        }
        return false;
    }
}
