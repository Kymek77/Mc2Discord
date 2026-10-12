package fr.denisd3d.mc2discord.core;

import com.electronwill.nightconfig.core.io.ParsingException;
import discord4j.common.ReactorResources;
import discord4j.common.close.CloseException;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.guild.MemberJoinEvent;
import discord4j.core.event.domain.guild.MemberLeaveEvent;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.gateway.GatewayReactorResources;
import discord4j.gateway.intent.Intent;
import discord4j.gateway.intent.IntentSet;
import discord4j.rest.http.client.ClientException;
import discord4j.rest.request.RouteMatcher;
import discord4j.rest.response.ResponseFunction;
import reactor.netty.resources.ConnectionProvider;
import fr.denisd3d.mc2discord.core.config.M2DConfig;
import fr.denisd3d.mc2discord.core.events.DiscordEvent;
import fr.denisd3d.mc2discord.core.events.LifecycleEvents;
import fr.denisd3d.mc2discord.core.storage.HiddenPlayerList;
import fr.denisd3d.mc2discord.core.storage.LinkedPlayerList;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Mc2Discord {
    public static final Logger LOGGER = LoggerFactory.getLogger("Mc2Discord");
    public static Mc2Discord INSTANCE;
    public GatewayDiscordClient client;
    public final IMinecraft minecraft;
    public final LangManager langManager = new LangManager();
    public M2DConfig config;
    public final List<String> errors = new ArrayList<>();
    public final Vars vars = new Vars();
    public final HiddenPlayerList hiddenPlayerList = new HiddenPlayerList();
    public final LinkedPlayerList linkedPlayerList = new LinkedPlayerList();

    public Mc2Discord(IMinecraft minecraft) {
        this.minecraft = minecraft;
        if (!loadConfig()) return;

        if (!M2DUtils.isTokenValid(this.config.general.token)) {
            LOGGER.error("Invalid Discord bot token");
            this.errors.add("Invalid Discord bot token");
            return;
        }

        ConnectionProvider connectionProvider = ConnectionProvider.builder("mc2discord")
                .maxConnections(500)
                .maxIdleTime(Duration.ofSeconds(45))
                .maxLifeTime(Duration.ofMinutes(5))
                .pendingAcquireTimeout(Duration.ofSeconds(60))
                .evictInBackground(Duration.ofSeconds(30))
                .build();

        ConnectionProvider gatewayConnectionProvider = ConnectionProvider.builder("d4j-gateway")
                .maxConnections(1)
                .maxIdleTime(Duration.ofSeconds(45))
                .maxLifeTime(Duration.ofMinutes(5))
                .pendingAcquireTimeout(Duration.ofSeconds(60))
                .evictInBackground(Duration.ofSeconds(30))
                .build();

        DiscordClient.builder(this.config.general.token)
                .setReactorResources(ReactorResources.builder()
                        .httpClient(ReactorResources.newHttpClient(connectionProvider)
                                .option(ChannelOption.SO_KEEPALIVE, true)
                                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                                .responseTimeout(Duration.ofSeconds(10))
                                .proxyWithSystemProperties())
                        .build())
                .onClientResponse(
                        ResponseFunction.retryWhen(
                                RouteMatcher.any(),
                                Retry.backoff(3, Duration.ofMillis(500))
                                        .filter(t -> t instanceof IOException || t instanceof TimeoutException || t instanceof java.util.concurrent.TimeoutException))
                )
                .build()
                .gateway()
                .setEnabledIntents(IntentSet.all().andNot(IntentSet.of(Intent.GUILD_PRESENCES)))
                .setGatewayReactorResources(reactorResources -> GatewayReactorResources.builder(reactorResources)
                        .httpClient(ReactorResources.newHttpClient(gatewayConnectionProvider)
                                .option(ChannelOption.SO_KEEPALIVE, true)
                                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                                .proxyWithSystemProperties())
                        .build())
                .login()
                .doOnError(CloseException.class, throwable -> {
                    Mc2Discord.LOGGER.error("Error while starting Discord bot: {} (CloseException, code {})", throwable.getCloseStatus().getReason().orElse(""), throwable.getCloseStatus().getCode());
                    this.errors.add("Error while starting Discord bot: " + throwable.getCloseStatus().getReason().orElse("") + " (code " + throwable.getCloseStatus().getCode() + ")");
                    if (throwable.getCloseStatus().getCode() == 4014) {
                        Mc2Discord.LOGGER.error("Make sure all required intents are enabled on Discord developer website (MESSAGE CONTENT & SERVER MEMBERS)");
                        this.errors.add("Make sure all required intents are enabled on Discord developer website (MESSAGE CONTENT & SERVER MEMBERS)");
                    }
                })
                .doOnError(ClientException.class, throwable -> {
                    Mc2Discord.LOGGER.error("Error while starting Discord bot: {} (ClientException, code {})", throwable.getStatus().reasonPhrase(), throwable.getStatus().code());
                    this.errors.add("Error while starting Discord bot: " + throwable.getStatus().reasonPhrase() + " (code " + throwable.getStatus().code() + ")");
                })
                .subscribe(gatewayDiscordClient -> {
                    this.client = gatewayDiscordClient;
                    this.client.on(ReadyEvent.class).subscribe(LifecycleEvents::onDiscordReady);
                    this.client.on(MessageCreateEvent.class).subscribe(DiscordEvent::onMessageCreate);
                    this.client.on(MemberJoinEvent.class).subscribe(DiscordEvent::onMemberJoin);
                    this.client.on(MemberLeaveEvent.class).subscribe(DiscordEvent::onMemberLeave);
                });
    }

    private boolean loadConfig() {
        try {
            if (new File("config").mkdir()) {
                LOGGER.info("Created config folder");
            }
            this.config = new M2DConfig(M2DUtils.CONFIG_FILE, translationKey -> {
                if ("config.lang.comment".equals(translationKey)) {
                    return langManager.formatMessage(translationKey, String.join(", ", LangManager.LANG_CONTRIBUTORS), String.join(", ", LangManager.AVAILABLE_LANG));
                }
                return minecraft.translateKey(langManager, translationKey);
            });

            this.config.loadAndCorrect();
        } catch (ParsingException parsingException) {
            config = null;
            this.errors.add("Config file is invalid: " + parsingException.getMessage());
            LOGGER.error("Config file is invalid:", parsingException);
        }

        return config != null;
    }

    public void restart() {
        shutdown().block();
        INSTANCE = new Mc2Discord(this.minecraft);
        INSTANCE.vars.modLoader = this.vars.modLoader;
        INSTANCE.vars.modLoaderVersion = this.vars.modLoaderVersion;
        INSTANCE.vars.modCount = this.vars.modCount;
    }

    public Mono<Void> shutdown() {
        StatusManager.stop();
        LoggingManager.stop();
        AccountManager.stop();
        MessageManager.WEBHOOK_CACHE.clear();

        return this.client != null ? this.client.logout() : Mono.empty();
    }
}
