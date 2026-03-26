package com.namelessmc.plugin.common;

import com.namelessmc.java_api.NamelessAPI;
import com.namelessmc.java_api.exception.ApiError;
import com.namelessmc.java_api.exception.ApiException;
import com.namelessmc.java_api.exception.NamelessException;
import com.namelessmc.plugin.common.audiences.NamelessPlayer;
import com.namelessmc.plugin.common.command.AbstractScheduledTask;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.spongepowered.configurate.ConfigurationNode;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class GroupSync implements Reloadable {

    /**
     * Current group set for linked players
     */
    private final Map<UUID, Set<String>> playerGroups = new ConcurrentHashMap<>();

    private final NamelessPlugin plugin;

    private @Nullable AbstractScheduledTask task = null;
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);
    private final AtomicBoolean syncQueued = new AtomicBoolean(false);
    private final AtomicBoolean rerunRequested = new AtomicBoolean(false);
    private final AtomicBoolean legacyGroupSyncRequired = new AtomicBoolean(false);
    private int serverId;

    GroupSync(final NamelessPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void unload() {
        if (this.task != null) {
            task.cancel();
        }
        this.task = null;
        this.syncQueued.set(false);
        this.rerunRequested.set(false);
        this.syncInProgress.set(false);
        this.legacyGroupSyncRequired.set(false);
    }

    @Override
    public void load() {
        ConfigurationNode config = this.plugin.config().main();
        if (!config.node("group-sync", "enabled").getBoolean()) {
            this.plugin.logger().fine("New group sync disabled");
            return;
        }

        this.plugin.logger().fine("Enabling new group sync system");

        this.serverId = config.node("api", "server-id").getInt(0);
        final Duration interval = ConfigurationHandler.getDuration(config.node("group-sync", "interval"));

        if (interval == null) {
            this.plugin.logger().warning("Group sync is enabled but the configured interval is invalid.");
            return;
        }

        if (this.serverId == 0) {
            this.plugin.logger().warning("Group sync is enabled but server-id is missing or zero. Group sync will not work until server-id is configured in main.yaml.");
            return;
        }

        final AbstractPermissions permissions = this.plugin.permissions();

        if (permissions == null) {
            this.plugin.logger().warning("Group sync is enabled, but no permissions adapter is active. Is a supported permissions system installed, like LuckPerms or Vault?");
            return;
        }

        if (!this.runAsyncSafely(() -> {
            try {
                final NamelessAPI api = this.plugin.apiProvider().api();
                if (api == null) {
                    return;
                }

                // Group sync API is available in 2.1.0+
                if (api.website().parsedVersion().minor() < 1) {
                    this.plugin.logger().warning("Website version is older than v2.1.0+, refusing to enable new group sync system");
                    return;
                }

                this.task = this.plugin.scheduler().runTimer(this::syncGroups, interval);
                this.requestSync();
            } catch (final NamelessException e) {
                this.plugin.logger().logException(e);
            }
        })) {
            this.plugin.logger().fine("Skipping group sync startup because the scheduler is no longer accepting tasks.");
        }
    }

    private void syncGroups() {
        this.syncQueued.set(false);

        if (this.plugin.isUnloading()) {
            this.syncInProgress.set(false);
            return;
        }

        final AbstractPermissions permissions = this.plugin.permissions();
        if (permissions == null) {
            throw new IllegalStateException("Permissions adapter cannot be null, or this task shouldn't have been registered");
        }

        if (!this.syncInProgress.compareAndSet(false, true)) {
            this.rerunRequested.set(true);
            return;
        }

        this.rerunRequested.set(false);

        final List<UUID> linkedMinecraftUuids = this.plugin.userCache().minecraftUuids();
        if (linkedMinecraftUuids.isEmpty()) {
            this.plugin.logger().fine("No linked Minecraft users in cache yet");
            this.finishSyncRun();
            return;
        }

        final Set<UUID> knownPlayers = new HashSet<>(linkedMinecraftUuids);
        this.playerGroups.keySet().removeIf(uuid -> !knownPlayers.contains(uuid));

        final Map<UUID, Set<String>> groupsToSend = Collections.synchronizedMap(new HashMap<>());
        final Map<UUID, GroupDelta> groupDiffs = Collections.synchronizedMap(new HashMap<>());
        final Map<UUID, Set<String>> latestGroups = Collections.synchronizedMap(new HashMap<>());
        final CompletableFuture<?>[] futures = linkedMinecraftUuids.stream()
                .map(uuid -> permissions.getPlayerGroups(uuid)
                        .thenAccept(newGroups -> {
                            if (newGroups == null) {
                                this.plugin.logger().fine(() -> "Cannot retrieve groups for player UUID: " + uuid);
                                return;
                            }

                            final Set<String> previousGroups = this.playerGroups.get(uuid);
                            if (previousGroups == null) {
                                this.plugin.logger().fine(() -> "Groups not previously known, or manually re-queued for player UUID: " + uuid);
                                latestGroups.put(uuid, newGroups);
                                groupsToSend.put(uuid, newGroups);
                                groupDiffs.put(uuid, new GroupDelta(newGroups, Collections.emptySet()));
                            } else if (!newGroups.equals(previousGroups)) {
                                this.plugin.logger().fine(() -> "Groups have changed for player UUID: " + uuid);
                                latestGroups.put(uuid, newGroups);
                                groupsToSend.put(uuid, newGroups);
                                final Set<String> addedGroups = new HashSet<>(newGroups);
                                addedGroups.removeAll(previousGroups);

                                final Set<String> removedGroups = new HashSet<>(previousGroups);
                                removedGroups.removeAll(newGroups);

                                groupDiffs.put(uuid, new GroupDelta(addedGroups, removedGroups));
                            }
                        }))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                this.plugin.logger().warning("An error occurred while collecting permission groups for group sync. The plugin will try again later.");
                this.plugin.logger().logException(throwable);
                this.finishSyncRun();
                return;
            }

            if (groupsToSend.isEmpty()) {
                this.finishSyncRun();
                return;
            }

            this.playerGroups.putAll(latestGroups);

            this.plugin.logger().fine(() -> "Sending groups for " + groupsToSend.size() + " players");

            if (!this.runAsyncSafely(() -> {
                try {
                    final NamelessAPI api = this.plugin.apiProvider().api();
                    if (api == null) {
                        return;
                    }
                    if (this.legacyGroupSyncRequired.get()) {
                        api.sendMinecraftGroups(this.serverId, groupsToSend);
                    } else {
                        try {
                            this.sendGroupDiffs(api, groupDiffs);
                        } catch (final ApiException e) {
                            if (this.shouldFallbackToLegacyGroupSync(e)) {
                                this.legacyGroupSyncRequired.set(true);
                                this.plugin.logger().fine("Website does not support the incremental Minecraft group sync request format, using the legacy bulk sync endpoint instead.");
                                api.sendMinecraftGroups(this.serverId, groupsToSend);
                            } else {
                                throw e;
                            }
                        }
                    }
                } catch (NamelessException e) {
                    if (this.wasAbortedDuringUnload(e)) {
                        this.plugin.logger().fine("Aborted group sync request because the plugin is unloading.");
                    } else {
                        this.plugin.logger().warning("An error occurred while sending player groups to the website, for group sync. The plugin will try again later.");
                        if (e instanceof ApiException && ((ApiException) e).apiError() == ApiError.CORE_INVALID_SERVER_ID) {
                            this.plugin.logger().warning("The server id configured in main.yaml is incorrect, or no correct group sync server is selected in StaffCP > Integrations > Minecraft > Minecraft Servers.");
                        } else {
                            this.plugin.logger().logException(e);
                        }
                    }

                    // Re-queue players by deleting their groups from local state
                    for (final UUID uuid : groupsToSend.keySet()) {
                        this.playerGroups.remove(uuid);
                    }
                } finally {
                    this.finishSyncRun();
                }
            })) {
                this.finishSyncRun();
            }
        });
    }

    private void sendGroupDiffs(final NamelessAPI api, final Map<UUID, GroupDelta> groupDiffs) throws NamelessException {
        for (final Map.Entry<UUID, GroupDelta> entry : groupDiffs.entrySet()) {
            final GroupDelta diff = entry.getValue();
            api.userByMinecraftUuidLazy(entry.getKey()).updateMinecraftGroups(
                    diff.addedGroups().toArray(String[]::new),
                    diff.removedGroups().toArray(String[]::new)
            );
        }
    }

    private boolean shouldFallbackToLegacyGroupSync(final ApiException e) {
        if (e.apiError() == ApiError.NAMELESS_INVALID_API_METHOD) {
            return true;
        }

        return e.apiError() == ApiError.NAMELESS_INVALID_POST_CONTENTS
                && e.getMessage().contains("\"field\":\"server_id\"");
    }

    private void finishSyncRun() {
        this.syncInProgress.set(false);
        if (!this.plugin.isUnloading() && this.rerunRequested.getAndSet(false)) {
            this.scheduleSyncIfNeeded();
        }
    }

    /**
     * Force groups to be sent again for this player
     * @param player
     */
    public void resetGroups(NamelessPlayer player) {
        this.resetGroups(player.uuid());
        this.requestSync();
    }

    public void resetGroups(UUID uuid) {
        this.playerGroups.remove(uuid);
    }

    public void requestSync() {
        if (this.task == null || this.plugin.isUnloading()) {
            return;
        }

        this.rerunRequested.set(true);
        this.scheduleSyncIfNeeded();
    }

    public void requestSync(UUID uuid) {
        this.requestSync();
    }

    private void scheduleSyncIfNeeded() {
        if (this.plugin.isUnloading() || this.syncInProgress.get() || !this.syncQueued.compareAndSet(false, true)) {
            return;
        }

        if (!this.runAsyncSafely(this::syncGroups)) {
            this.syncQueued.set(false);
        }
    }

    private boolean runAsyncSafely(final Runnable runnable) {
        if (this.plugin.isUnloading()) {
            return false;
        }

        try {
            this.plugin.scheduler().runAsync(runnable);
            return true;
        } catch (final RejectedExecutionException ignored) {
            return false;
        }
    }

    private boolean wasAbortedDuringUnload(final NamelessException e) {
        return this.plugin.isUnloading()
                && (e.getCause() instanceof InterruptedException || "In-progress request was aborted".equals(e.getMessage()));
    }

    private static final class GroupDelta {
        private final Set<String> addedGroups;
        private final Set<String> removedGroups;

        private GroupDelta(final Set<String> addedGroups, final Set<String> removedGroups) {
            this.addedGroups = addedGroups;
            this.removedGroups = removedGroups;
        }

        private Set<String> addedGroups() {
            return this.addedGroups;
        }

        private Set<String> removedGroups() {
            return this.removedGroups;
        }
    }

}
