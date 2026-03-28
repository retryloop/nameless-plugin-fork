package com.namelessmc.plugin.common;

import com.namelessmc.plugin.common.audiences.NamelessPlayer;
import net.luckperms.api.LuckPerms;
import net.luckperms.api.LuckPermsProvider;
import net.luckperms.api.event.EventSubscription;
import net.luckperms.api.event.user.UserDataRecalculateEvent;
import net.luckperms.api.model.group.Group;
import net.luckperms.api.model.user.User;
import net.luckperms.api.node.NodeType;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.query.QueryOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LuckPermsPermissions extends AbstractPermissions {

	private final NamelessPlugin plugin;
	private final Set<UUID> ignoredRecalculateUsers = ConcurrentHashMap.newKeySet();
	private @Nullable LuckPerms api;
	private @Nullable EventSubscription<UserDataRecalculateEvent> userRecalculateSubscription;

	public LuckPermsPermissions(final NamelessPlugin plugin) {
		this.plugin = plugin;
	}

	@Override
	public void unload() {
		if (this.userRecalculateSubscription != null) {
			this.userRecalculateSubscription.close();
			this.userRecalculateSubscription = null;
		}
		this.ignoredRecalculateUsers.clear();
		this.api = null;
	}

	@Override
	public void load() {
		try {
			this.api = LuckPermsProvider.get();
			this.userRecalculateSubscription = this.api.getEventBus().subscribe(UserDataRecalculateEvent.class,
					event -> {
						final UUID uuid = event.getUser().getUniqueId();
						if (!this.ignoredRecalculateUsers.contains(uuid)) {
							this.plugin.groupSync().requestSync(uuid);
						}
					});
		} catch (IllegalStateException | NoClassDefFoundError e) {}
	}

	@Override
	public boolean isUsable() {
		return this.api != null;
	}

	@Override
	public int priority() {
		return 100;
	}

	@Override
	public Set<String> getGroups() {
		if (this.api == null) {
			throw new ProviderNotUsableException();
		}
		return this.api.getGroupManager().getLoadedGroups().stream()
				.map(Group::getName)
				.collect(Collectors.toUnmodifiableSet());
	}

	@Override
	public @Nullable Set<String> getPlayerGroups(NamelessPlayer player) {
		if (this.api == null) {
			throw new ProviderNotUsableException();
		}
		final User user = this.api.getUserManager().getUser(player.uuid());
		if (user == null) {
			return null;
		}
		return this.collectActiveGroupNames(user);
	}

	@Override
	public CompletableFuture<@Nullable Set<String>> getPlayerGroups(final UUID uuid) {
		if (this.api == null) {
			throw new ProviderNotUsableException();
		}
		this.ignoredRecalculateUsers.add(uuid);
		return this.api.getUserManager().loadUser(uuid)
				.thenApply(this::collectActiveGroupNames)
				.whenComplete((ignored, throwable) -> {
					if (this.plugin.isUnloading()) {
						this.ignoredRecalculateUsers.remove(uuid);
						return;
					}

					this.plugin.scheduler().runDelayed(
							() -> this.ignoredRecalculateUsers.remove(uuid),
							Duration.ofSeconds(2)
					);
				});
	}

	private Set<String> collectActiveGroupNames(final User user) {
		user.auditTemporaryNodes();
		final QueryOptions queryOptions = user.getQueryOptions();

		return user.getNodes(NodeType.INHERITANCE).stream()
				.filter(InheritanceNode::getValue)
				.filter(node -> !node.hasExpired())
				.filter(node -> queryOptions.satisfies(node.getContexts()))
				.map(InheritanceNode::getGroupName)
				.collect(Collectors.toSet());
	}
}
